use std::collections::BTreeMap;
use std::ops::{Index, Range};
use bytes::{BufMut, BytesMut};
use crate::{byte_slice_to_u64, extractRowIdFromDataKey, extractTxIdFromMvccKey, extractTxIdFromPointerKey, global,
            keyPrefixAddRowId, meta, throw, u64ToByteArrRef};
use crate::executor::CommandExecutor;
use crate::types::{Byte, ColumnFamily, DataKey, DBRawIterator, KeyTag, KV, RowId, TableId, TxId};

impl<'session> CommandExecutor<'session> {
    // 对data对应的mvccKey的visibility筛选
    pub fn checkCommittedDataVisibilityWithoutTxMutations(&self,
                                                          mvccKeyBuffer: &mut BytesMut,
                                                          rawIterator: &mut DBRawIterator,
                                                          dataKey: DataKey,
                                                          columnFamily: &ColumnFamily,
                                                          tableName: &String) -> anyhow::Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // xmin
        // 当vaccum时候会变为 TX_ID_FROZEN 别的时候不会变动 只会有1条
        mvccKeyBuffer.writeDataMvccXmin(dataKey, meta::TX_ID_FROZEN);
        rawIterator.seek(mvccKeyBuffer.as_ref());
        // rawIterator生成的时候可以通过readOption设置bound 要是越过的话iterator.valid()为false
        let mvccKeyXmin = rawIterator.key().unwrap();
        let xmin = extractTxIdFromMvccKey!(mvccKeyXmin);

        // xmax
        mvccKeyBuffer.writeDataMvccXmax(dataKey, currentTxId);
        rawIterator.seek_for_prev(mvccKeyBuffer.as_ref());
        // 能确保会至少有xmax是0的 mvcc条目
        let mvccKeyXmax = rawIterator.key().unwrap();
        let xmax = extractTxIdFromMvccKey!(mvccKeyXmax);

        let snapshot = self.session.getSnapshot()?;

        // 应对多个tx对相同rowId的数据update而产生的多条新data
        let originDataKeyKey = u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PPREFIX_ORIGIN_DATA_KEY, extractRowIdFromDataKey!(dataKey)));
        let originDataKey = snapshot.get_cf(columnFamily, originDataKeyKey)?.unwrap();
        let originDataKey = byte_slice_to_u64!(originDataKey);
        // 说明本条data是通过update而来 老data的dataKey是originDataKey
        if meta::DATA_KEY_INVALID != originDataKey {
            // 探寻originDataKey对应的mvcc xmax记录
            mvccKeyBuffer.writeDataMvccXmax(originDataKey, currentTxId);
            rawIterator.seek_for_prev(mvccKeyBuffer.as_ref());

            // 能确保会至少有xmax是0的 mvcc条目
            // 得知本tx可视范围内该条老data是recently被哪个tx干掉的
            let originDataXmax = extractTxIdFromMvccKey!( rawIterator.key().unwrap());
            // 要和本条data的xmin比较 如果不相等的话抛弃
            if xmin != originDataXmax {
                // todo 还需要把这条因为update产生的多的new data 干掉 完成
                let xmax = self.generateDeleteDataXmax(mvccKeyBuffer, dataKey)?;
                self.session.writeDeleteDataMutation(tableName, xmax);
                return Ok(false);
            }
        }

        Ok(meta::isVisible(currentTxId, xmin, xmax))
    }

    pub fn checkCommittedDataVisibilityWithTxMutations(&self,
                                                       mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                                       mvccKeyBuffer: &mut BytesMut,
                                                       dataKey: DataKey) -> anyhow::Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 要看落地的有没有被当前的tx上的干掉  只要读取相应的xmax的mvccKey
        // mutationsRawCurrentTx的txId只会是currentTxId
        mvccKeyBuffer.writeDataMvccXmax(dataKey, currentTxId);

        Ok(mutationsRawCurrentTx.get(mvccKeyBuffer.as_ref()).is_none())
    }

    pub fn checkUncommittedDataVisibility(&self,
                                          mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                          mvccKeyBuffer: &mut BytesMut,
                                          addedDataKeyCurrentTx: DataKey) -> anyhow::Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 检验当前tx上新add的话 只要检验相应的xmax便可以了 就算有xmax那对应的txId也只会是currentTx
        mvccKeyBuffer.writeDataMvccXmax(addedDataKeyCurrentTx, currentTxId);

        // 说明这个当前tx上insert的data 后来又被当前tx的干掉了
        Ok(mutationsRawCurrentTx.get(mvccKeyBuffer.as_ref()).is_none())
    }

    // todo  pointerKey如何应对mvcc 完成
    /// 因为mvcc信息直接是在pointerKey上的 去看它的末尾的xmax
    pub fn checkCommittedPointerVisibilityWithoutCurrentTxMutations(&self,
                                                                    pointerKeyBuffer: &mut BytesMut,
                                                                    rawIterator: &mut DBRawIterator,
                                                                    committedPointerKey: &[Byte]) -> anyhow::Result<bool> {
        let currentTxId = self.session.getTxId()?;

        const RANGE: Range<usize> = meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET..meta::POINTER_KEY_BYTE_LEN;

        // pointerKey末尾的 mvccKeyTag和txId
        let pointerKeyTail = committedPointerKey.index(RANGE);

        // 读取 mvccKeyTag
        match *pointerKeyTail.first().unwrap() {
            // 含有xmin的pointerKey 抛弃掉不要,是没有问题的因为相应的指向信息在xmax的pointerKey也有
            meta::MVCC_KEY_TAG_XMIN => Ok(false),
            meta::MVCC_KEY_TAG_XMAX => {
                let xmax = byte_slice_to_u64!(&pointerKeyTail[1..]) as TxId;

                if currentTxId >= xmax {
                    if (xmax == meta::TX_ID_INVALID) == false {
                        return Ok(false);
                    }
                }

                // 到了这边说明 满足 xmax == 0 || xmax > currentTxId
                // 到了这边说明该pointerKey本身单独看是没有问题的 不过还需要联系后边是不是会干掉
                // 要是后边还有 currentTxId > xmax 的 就需要应对
                let prefix = &committedPointerKey[..meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET];

                // 生成xmax是currentTxId 的pointerKey
                pointerKeyBuffer.replacePointerKeyMcvvTagTxId(committedPointerKey, meta::MVCC_KEY_TAG_XMIN, meta::TX_ID_FROZEN);
                rawIterator.seek(pointerKeyBuffer.as_ref());
                let xmin = extractTxIdFromPointerKey!(rawIterator.key().unwrap());

                pointerKeyBuffer.replacePointerKeyMcvvTagTxId(committedPointerKey, meta::MVCC_KEY_TAG_XMAX, currentTxId);
                rawIterator.seek_for_prev(pointerKeyBuffer.as_ref());
                let xmax = extractTxIdFromPointerKey!(rawIterator.key().unwrap());

                Ok(meta::isVisible(currentTxId, xmin, xmax))
            }
            _ => panic!("impossible")
        }
    }

    pub fn checkCommittedPointerVisibilityWithCurrentTxMutations(&self,
                                                                 mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                                                 pointerKeyBuffer: &mut BytesMut,
                                                                 committedPointerKey: &[Byte]) -> anyhow::Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 要是当前的tx干掉的话会有这样的xmax
        pointerKeyBuffer.replacePointerKeyMcvvTagTxId(committedPointerKey, meta::MVCC_KEY_TAG_XMAX, currentTxId);

        Ok(mutationsRawCurrentTx.get(pointerKeyBuffer.as_ref()).is_none())
    }

    pub fn checkUncommittedPointerVisibility(&self,
                                             mutationsRawCurrentTx: &BTreeMap<Vec<Byte>, Vec<Byte>>,
                                             pointerKeyBuffer: &mut BytesMut,
                                             addedPointerKeyCurrentTx: &[Byte]) -> anyhow::Result<bool> {
        let currentTxId = self.session.getTxId()?;

        // 要是当前的tx干掉的话会有这样的xmax
        pointerKeyBuffer.replacePointerKeyMcvvTagTxId(addedPointerKeyCurrentTx, meta::MVCC_KEY_TAG_XMAX, currentTxId);

        Ok(mutationsRawCurrentTx.get(pointerKeyBuffer.as_ref()).is_none())
    }

    /// 当前tx上add时候生成 xmin xmax 对应的mvcc key
    pub fn generateAddDataXminXmax(&self, mvccKeyBuffer: &mut BytesMut, dataKey: DataKey) -> anyhow::Result<(KV, KV)> {
        let xmin = {
            mvccKeyBuffer.writeDataMvccXmin(dataKey, self.session.getTxId()?);
            (mvccKeyBuffer.to_vec(), global::EMPTY_BINARY)
        };

        let xmax = {
            mvccKeyBuffer.writeDataMvccXmax(dataKey, meta::TX_ID_INVALID);
            (mvccKeyBuffer.to_vec(), global::EMPTY_BINARY)
        };

        Ok((xmin, xmax))
    }

    /// 当前tx上delete时候生成 xmax的 mvccKey
    pub fn generateDeleteDataXmax(&self, mvccKeyBuffer: &mut BytesMut, dataKey: DataKey) -> anyhow::Result<KV> {
        mvccKeyBuffer.writeDataMvccXmax(dataKey, self.session.getTxId()?);
        Ok((mvccKeyBuffer.to_vec(), global::EMPTY_BINARY))
    }

    pub fn generateOrigin(&self, selfDataKey: DataKey, originDataKey: DataKey) -> KV {
        let selfRowId = extractRowIdFromDataKey!(selfDataKey);
        (
            u64ToByteArrRef!(keyPrefixAddRowId!(meta::KEY_PPREFIX_ORIGIN_DATA_KEY, selfRowId)).to_vec(),
            u64ToByteArrRef!(originDataKey).to_vec()
        )
    }

    /// 当前tx上link的时候 生成的含有xmin 和 xmax 的pointerKey
    pub fn generateAddPointerXminXmax(&self,
                                      pointerKeyBuffer: &mut BytesMut,
                                      selfDataKey: DataKey,
                                      pointerKeyTag: KeyTag, tableId: TableId, targetDatakey: DataKey) -> anyhow::Result<(KV, KV)> {
        let xmin = {
            pointerKeyBuffer.writePointerKeyMvccXmin(selfDataKey, pointerKeyTag, tableId, targetDatakey, self.session.getTxId()?);
            (pointerKeyBuffer.to_vec(), global::EMPTY_BINARY) as KV
        };

        let xmax = {
            pointerKeyBuffer.writePointerKeyMvccXmax(selfDataKey, pointerKeyTag, tableId, targetDatakey, meta::TX_ID_INVALID);
            (pointerKeyBuffer.to_vec(), global::EMPTY_BINARY) as KV
        };

        Ok((xmin, xmax))
    }

    /// 当前tx上unlink时候 生成的含有xmax的 pointerKey
    pub fn generateDeletePointerXmax(&self,
                                     pointerKeyBuffer: &mut BytesMut,
                                     selfDataKey: DataKey,
                                     pointerKeyTag: KeyTag, tableId: TableId, targetDatakey: DataKey) -> anyhow::Result<KV> {
        pointerKeyBuffer.writePointerKeyMvccXmax(selfDataKey, pointerKeyTag, tableId, targetDatakey, self.session.getTxId()?);
        Ok((pointerKeyBuffer.to_vec(), global::EMPTY_BINARY))
    }
}

pub trait BytesMutExt {
    // todo writePointerKeyBuffer() 和 writePointerKeyLeadingPart() 有公用部分的 完成
    /// 不包含末尾的对应其它table rel 的dataKey
    fn writePointerKeyLeadingPart(&mut self,
                                  dataKey: DataKey,
                                  keyTag: KeyTag, tableId: TableId);

    // ----------------------------------------------------------------------------

    fn replacePointerKeyMcvvTagTxId(&mut self, pointerKey: &[Byte], mvccKeyTag: KeyTag, txId: TxId);

    fn writePointerKeyMvccXmin(&mut self,
                               selfDatakey: DataKey,
                               pointerKeyTag: KeyTag, targetTableId: TableId, targetDataKey: DataKey,
                               txId: TxId) {
        self.writePointerKey(selfDatakey, pointerKeyTag, targetTableId, targetDataKey, meta::MVCC_KEY_TAG_XMIN, txId)
    }

    fn writePointerKeyMvccXmax(&mut self,
                               selfDatakey: DataKey,
                               pointerKeyTag: KeyTag, targetTableId: TableId, targetDataKey: DataKey,
                               txId: TxId) {
        self.writePointerKey(selfDatakey, pointerKeyTag, targetTableId, targetDataKey, meta::MVCC_KEY_TAG_XMAX, txId)
    }

    fn writePointerKey(&mut self,
                       selfDatakey: DataKey,
                       pointerKeyTag: KeyTag, targetTableId: TableId, targetDataKey: DataKey,
                       pointerMvccKeyTag: KeyTag, txId: TxId);

    // --------------------------------------------------------------------------------

    fn writeDataMvccXmin(&mut self, dataKey: DataKey, xmin: TxId) {
        self.writeDataMvccKey(dataKey, meta::MVCC_KEY_TAG_XMIN, xmin).unwrap();
    }

    fn writeDataMvccXminByRowId(&mut self, rowId: RowId, xmin: TxId) {
        self.writeDataMvccKeyByRowId(rowId, meta::MVCC_KEY_TAG_XMIN, xmin).unwrap();
    }

    fn writeDataMvccXmax(&mut self, dataKey: DataKey, xmax: TxId) {
        self.writeDataMvccKey(dataKey, meta::MVCC_KEY_TAG_XMAX, xmax).unwrap()
    }

    fn writeDataMvccKey(&mut self,
                        dataKey: DataKey,
                        mvccKeyTag: KeyTag, txid: TxId) -> anyhow::Result<()> {
        let rowId = extractRowIdFromDataKey!(dataKey);
        self.writeDataMvccKeyByRowId(rowId, mvccKeyTag, txid)
    }

    fn writeDataMvccKeyByRowId(&mut self,
                               rowId: RowId,
                               mvccKeyTag: KeyTag, txid: TxId) -> anyhow::Result<()>;
}

impl BytesMutExt for BytesMut {
    fn writePointerKeyLeadingPart(&mut self,
                                  selfDataKey: DataKey,
                                  keyTag: KeyTag, targetTableId: TableId) {
        self.clear();

        let rowId = extractRowIdFromDataKey!(selfDataKey);
        self.put_u64(keyPrefixAddRowId!(meta::KEY_PREFIX_POINTER, rowId));

        // 写relation的tableId
        self.put_u8(keyTag);
        self.put_u64(targetTableId);

        // 后边用来写dataKey
        self.put_u8(meta::POINTER_KEY_TAG_DATA_KEY);
    }

    fn replacePointerKeyMcvvTagTxId(&mut self, pointerKey: &[Byte], mvccKeyTag: KeyTag, txId: TxId) {
        self.clear();

        self.put_slice(&pointerKey[..meta::POINTER_KEY_MVCC_KEY_TAG_OFFSET]);

        self.put_u8(mvccKeyTag);
        self.put_u64(txId);
    }

    fn writePointerKey(&mut self,
                       selfDatakey: DataKey,
                       pointerKeyTag: KeyTag, tableId: TableId, dataKey: DataKey,
                       pointerMvccKeyTag: KeyTag, txId: TxId) {
        self.writePointerKeyLeadingPart(selfDatakey, pointerKeyTag, tableId);
        self.put_u64(dataKey);
        self.put_u8(pointerMvccKeyTag);
        self.put_u64(txId);
    }

    fn writeDataMvccKeyByRowId(&mut self,
                               rowId: RowId,
                               mvccKeyTag: KeyTag, txid: TxId) -> anyhow::Result<()> {
        self.clear();

        match mvccKeyTag {
            meta::MVCC_KEY_TAG_XMIN | meta::MVCC_KEY_TAG_XMAX => {
                self.put_u64(keyPrefixAddRowId!(meta::KEY_PREFIX_MVCC, rowId));
                self.put_u8(mvccKeyTag);
                self.put_u64(txid);
            }
            _ => throw!("should be KEY_PREFIX_MVCC_XMIN, KEY_PREFIX_MVCC_XMAX"),
        }

        Ok(())
    }
}