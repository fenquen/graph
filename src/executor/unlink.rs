use bytes::BytesMut;
use rocksdb::{Direction, IteratorMode};
use crate::{extractTargetDataKeyFromPointerKey, meta, byte_slice_to_u64, types};
use crate::executor::{CommandExecResult, CommandExecutor};
use crate::executor::mvcc::BytesMutExt;
use crate::executor::store::{ScanHooks, ScanParams};
use crate::parser::command::unlink::{Unlink, UnlinkLinkStyle, UnlinkSelfStyle};
use crate::session::Session;
use crate::types::{ColumnFamily, DataKey, CommittedPreProcessor};

impl<'session> CommandExecutor<'session> {
    // todo pointer指向点和边的xmin xmax如何应对
    pub(super) fn unlink(&self, unlink: &Unlink) -> anyhow::Result<CommandExecResult> {
        match unlink {
            Unlink::LinkStyle(unlinkLinkStyle) => self.unlinkLinkStyle(unlinkLinkStyle),
            Unlink::SelfStyle(unlinkSelfStyle) => self.unlinkSelfStyle(unlinkSelfStyle),
        }
    }

    /// 应对 unlink user(id > 1 and (name in ('a') or code = null)) to car(color='red') by usage(number = 13) 和原来link基本相同
    /// 和原来的link套路相同是它反过来的
    fn unlinkLinkStyle(&self, unlinkLinkStyle: &UnlinkLinkStyle) -> anyhow::Result<CommandExecResult> {
        let relation = self.getDBObjectByName(&unlinkLinkStyle.relationName)?;
        let relation = relation.asRelation()?;

        let destTable = self.getDBObjectByName(&unlinkLinkStyle.destTableName)?;
        let destTable = destTable.asTable()?;

        let srcTable = self.getDBObjectByName(&unlinkLinkStyle.srcTableName)?;
        let srcTable = srcTable.asTable()?;

        let relationColumnFamily = Session::getColFamily(unlinkLinkStyle.relationName.as_str())?;

        // 得到rel 干掉指向src和dest的pointer key
        let relationRowDatas = {
            let scanParams = ScanParams {
                table: relation,
                tableFilter: unlinkLinkStyle.relationFilterExpr.as_ref(),
                ..Default::default()
            };

            self.scanSatisfiedRows(scanParams, true, ScanHooks::default())?
        };

        // KEY_PREFIX_POINTER + relDataRowId + KEY_TAG_SRC_TABLE_ID + src的tableId + KEY_TAG_KEY + src dest rel的dataKey
        let mut pointerKeyBuffer = self.withCapacityIn(meta::POINTER_KEY_BYTE_LEN);

        let snapshot = self.session.getSnapshot()?;

        let mut processRelationRowData =
            |statisfiedRelDataKey: DataKey, processSrc: bool| -> anyhow::Result<()> {
                if processSrc {
                    pointerKeyBuffer.writePointerKeyLeadingPart(statisfiedRelDataKey,
                                                                meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.id);
                } else {
                    pointerKeyBuffer.writePointerKeyLeadingPart(statisfiedRelDataKey,
                                                                meta::POINTER_KEY_TAG_DEST_TABLE_ID, srcTable.id);
                }


                for result in snapshot.iterator_cf(&relationColumnFamily, IteratorMode::From(pointerKeyBuffer.as_ref(), Direction::Forward)) {
                    let (relPointerKey, _) = result?;

                    // iterator是收不动尾部的要自个来弄的
                    if relPointerKey.starts_with(pointerKeyBuffer.as_ref()) == false {
                        break;
                    }

                    // relation的src dest 的dataKey
                    let targetDataKey = extractTargetDataKeyFromPointerKey!(&*relPointerKey);

                    let mut scanParams = ScanParams::default();
                    scanParams.table = srcTable;
                    scanParams.tableFilter = if processSrc {
                        unlinkLinkStyle.srcTableFilterExpr.as_ref()
                    } else {
                        unlinkLinkStyle.destTableFilterExpr.as_ref()
                    };
                    scanParams.selectedColumnNames = None;

                    // 是不是符合src dest上的筛选expr
                    if self.getRowDatasByDataKeys(&[targetDataKey], &scanParams, &mut ScanHooks::default())?.is_empty() {
                        continue;
                    }

                    if processSrc {
                        // 干掉src上的对应该rel的pointerKey
                        let oldXmax =
                            self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                           targetDataKey,
                                                           meta::POINTER_KEY_TAG_DOWNSTREAM_REL_ID, relation.id, statisfiedRelDataKey)?;
                        self.session.writeDeletePointerMutation(&srcTable.name, oldXmax);

                        // 干掉rel上对应该src的pointerKey
                        let oldXmax =
                            self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                           statisfiedRelDataKey,
                                                           meta::POINTER_KEY_TAG_SRC_TABLE_ID, srcTable.id, targetDataKey)?;
                        self.session.writeDeletePointerMutation(&relation.name, oldXmax);
                    } else {
                        // 干掉dest上的对应该rel的pointerKey
                        let oldXmax =
                            self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                           targetDataKey,
                                                           meta::POINTER_KEY_TAG_UPSTREAM_REL_ID, relation.id, statisfiedRelDataKey)?;
                        self.session.writeDeletePointerMutation(&destTable.name, oldXmax);

                        // 干掉rel上对应该dest的pointerKey
                        let oldXmax =
                            self.generateDeletePointerXmax(&mut pointerKeyBuffer,
                                                           statisfiedRelDataKey,
                                                           meta::POINTER_KEY_TAG_DEST_TABLE_ID, destTable.id, targetDataKey)?;
                        self.session.writeDeletePointerMutation(&relation.name, oldXmax);
                    }
                }

                Ok(())
            };

        // 遍历符合要求的relRowData 得到单个上边的对应src和dest的全部dataKeys
        for (relationRowData, _) in relationRowDatas {
            // src
            processRelationRowData(relationRowData, true)?;

            // dest
            processRelationRowData(relationRowData, false)?;  // 遍历符合要求的relRowData 得到单个上边的对应src和dest的全部dataKeys
        }

        Ok(CommandExecResult::DmlResult)
    }

    /// unlink user(id >1 ) as start by usage (number = 7) ,as end by own(number =7)
    /// 需要由start点出发
    fn unlinkSelfStyle(&self, unlinkSelfStyle: &UnlinkSelfStyle) -> anyhow::Result<CommandExecResult> {
        Ok(CommandExecResult::DmlResult)
    }
}