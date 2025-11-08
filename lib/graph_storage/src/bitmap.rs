use std::mem::size_of;

/// 64路btree
/// 最底部数量是实际的数量,再向上1个level的数量是1/64
/// 上级的1个元素 代表了 下级的64个元素的缩略情况
/// 如果下级的64个元素全是1(已占用) 那么它是1 不然它是0(还有未占用的可能)
pub(crate) struct BtreeBitmap {
    levels: Vec<Bitmap>,
}

impl BtreeBitmap {
    pub(crate) fn new(mut count: usize) -> Self {
        let mut levels = vec![];

        // Build from the leaf to root
        loop {
            levels.push(Bitmap::new(count));

            if count <= 64 {
                break;
            }

            // 上级元素的数量是下级的1/64
            count = count.div_ceil(64);
        }

        levels.reverse();

        BtreeBitmap {
            levels
        }
    }

    fn lastLevel(&self) -> &Bitmap {
        self.level(self.levels.len() - 1)
    }

    fn lastLevelMut(&mut self) -> &mut Bitmap {
        self.levelMut(self.levels.len() - 1)
    }

    fn level(&self, i: usize) -> &Bitmap {
        &self.levels[i]
    }

    fn levelMut(&mut self, i: usize) -> &mut Bitmap {
        &mut self.levels[i]
    }

    pub(crate) fn unsetCount(&self) -> usize {
        self.lastLevel().unsetCount()
    }

    pub(crate) fn hasAnyUnset(&self) -> bool {
        self.lastLevel().hasAnyUnset()
    }

    pub(crate) fn get(&self, elemIndex: usize) -> bool {
        self.lastLevel().get(elemIndex)
    }

    pub(crate) fn elementCount(&self) -> usize {
        self.lastLevel().elementCount
    }

    pub(crate) fn find1stUnset(&self) -> Option<usize> {
        if let Some(mut entry) = self.level(0).find1stUnsetIndex(0, 64) {
            let mut height = 0;

            // 不断下钻到底
            while height < self.levels.len() - 1 {
                height += 1;
                entry *= 64;
                entry = self.level(height).find1stUnsetIndex(entry, entry + 64).unwrap();
            }

            Some(entry)
        } else {
            None
        }
    }

    pub(crate) fn alloc(&mut self) -> Option<usize> {
        self.find1stUnset().map(|index| {
            self.set(index);
            index
        })
    }

    pub(crate) fn set(&mut self, elemIndex: usize) {
        let full = self.lastLevelMut().set(elemIndex);
        self.update2Root(elemIndex, full);
    }

    pub(crate) fn clear(&mut self, elemIndex: usize) {
        self.lastLevelMut().clear(elemIndex);
        self.update2Root(elemIndex, false);
    }

    // Recursively update to the root, starting at the given entry in the given height
    // full parameter must be set if all bits in the entry's group of u64 are full
    fn update2Root(&mut self, i: usize, mut full: bool) {
        if self.levels.len() == 1 {
            return;
        }

        let mut parentLevel = self.levels.len() - 2;
        let mut parent_entry = i / 64;

        loop {
            full = if full {
                self.levelMut(parentLevel).set(parent_entry)
            } else {
                self.levelMut(parentLevel).clear(parent_entry);
                false
            };

            if parentLevel == 0 {
                break;
            }

            parentLevel -= 1;
            parent_entry /= 64;
        }
    }

    const LEVEL_COUNT_BYTE_LEN: usize = size_of::<usize>();
    const LEVEL_BINARY_END_POS_BYTE_LEN: usize = size_of::<usize>();

    /// [level数量 4字节] [level0的binary的末尾position 4字节] [level1的binary的末尾position 4字节] [level0的binary] [level1的binary]
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut binary = vec![];

        // 4字节 level数量
        binary.extend(self.levels.len().to_be_bytes());

        let levelBinaries: Vec<Vec<u8>> = self.levels.iter().map(|bitmap| bitmap.serialize()).collect();

        // 4字节 单个level的data末尾的position
        {
            let mut levelBinaryEndPos = Self::LEVEL_COUNT_BYTE_LEN + self.levels.len() * Self::LEVEL_BINARY_END_POS_BYTE_LEN;

            for levelBinary in &levelBinaries {
                levelBinaryEndPos += levelBinary.len();
                binary.extend(levelBinaryEndPos.to_be_bytes());
            }
        }

        for levelBinary in &levelBinaries {
            binary.extend(levelBinary);
        }

        binary
    }

    pub(crate) fn refresh(&self, dest: &mut [u8]) {
        let mut levelBinaryEndPosPos = Self::LEVEL_COUNT_BYTE_LEN;
        let mut levelBinaryStartPos = levelBinaryEndPosPos + (self.levels.len()) * Self::LEVEL_BINARY_END_POS_BYTE_LEN;

        for bitmap in self.levels.iter() {
            let levelBinaryEndPos = usize::from_be_bytes(dest[levelBinaryEndPosPos..(levelBinaryEndPosPos + Self::LEVEL_BINARY_END_POS_BYTE_LEN)].try_into().unwrap());
            bitmap.refresh(&mut dest[levelBinaryStartPos..levelBinaryEndPos]);

            // 上个的终点是下个的起点
            levelBinaryStartPos = levelBinaryEndPos;
            levelBinaryEndPosPos += Self::LEVEL_BINARY_END_POS_BYTE_LEN;
        }
    }

    pub(crate) fn deserialize(binary: &[u8]) -> BtreeBitmap {
        let levelCount = usize::from_be_bytes(binary[..Self::LEVEL_COUNT_BYTE_LEN].try_into().unwrap());

        let mut levelBinaryEndPosPos = Self::LEVEL_COUNT_BYTE_LEN;
        let mut levelBinaryStartPos = levelBinaryEndPosPos + (levelCount) * Self::LEVEL_BINARY_END_POS_BYTE_LEN;

        let mut levels = vec![];

        for _ in 0..levelCount {
            let levelBinaryEndPos = usize::from_be_bytes(binary[levelBinaryEndPosPos..(levelBinaryEndPosPos + Self::LEVEL_BINARY_END_POS_BYTE_LEN)].try_into().unwrap());
            levels.push(Bitmap::deserialize(&binary[levelBinaryStartPos..levelBinaryEndPos]));

            // 上个的终点是下个的起点
            levelBinaryStartPos = levelBinaryEndPos;
            levelBinaryEndPosPos += Self::LEVEL_BINARY_END_POS_BYTE_LEN;
        }

        BtreeBitmap {
            levels
        }
    }
}

pub(crate) struct Bitmap {
    elementCount: usize,
    words: Vec<u64>,
}

impl Bitmap {
    const ELEMENT_COUNT_BYTE_LEN: usize = size_of::<usize>();
    const WORD_BYTE_LEN: usize = size_of::<u64>();

    /// 需要多少个u64
    fn wordCount(bitCount: usize) -> usize {
        bitCount.div_ceil(64)
    }

    pub fn newFull(elementCount: usize) -> Bitmap {
        Bitmap {
            elementCount,
            words: vec![u64::MAX; Self::wordCount(elementCount)], // 单个u64的64个bit全是1
        }
    }

    pub fn new(elementCount: usize) -> Self {
        Self {
            elementCount,
            words: vec![0; Self::wordCount(elementCount)], // 单个u64的64个bit全是0
        }
    }

    /// 这个bit对应的u64的index和bit在这个u64的index的
    fn wordIndexBitIndexInWord(elemIndex: usize) -> (usize, usize) {
        ((elemIndex) / Self::WORD_BYTE_LEN, (elemIndex) % Self::WORD_BYTE_LEN)
    }

    fn getSetMask(bitIndex: usize) -> u64 {
        1u64 << (bitIndex as u64)
    }

    fn unsetCount(&self) -> usize {
        self.words.iter().map(|x| x.count_zeros() as usize).sum()
    }

    pub fn difference<'a0, 'b0>(&'a0 self, exclusion: &'b0 Bitmap) -> BitmapDifference<'a0, 'b0> {
        BitmapDifference::new(&self.words, &exclusion.words)
    }

    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter::new(self.elementCount, &self.words)
    }

    fn hasAnyUnset(&self) -> bool {
        self.words.iter().any(|word| word.count_zeros() > 0)
    }

    fn find1stUnsetIndex(&self, startBit: usize, endBit: usize) -> Option<usize> {
        // 确保endBit是离startBit最近的64的倍数
        assert_eq!(endBit, (startBit - startBit % 64) + 64);

        let (wordIndex, bitIndexInWord) = Self::wordIndexBitIndexInWord(startBit);

        // 注意bit位的顺序是从右往左数的
        // 01110000
        // startBit: 1
        // bitIndexInWord: 1
        // mask: 00000001
        // group: 01110001
        // trailingOneCount: 1
        let mask = (1 << bitIndexInWord) - 1;
        let group = (self.words[wordIndex]) | mask;

        match group.trailing_ones() {
            64 => None,
            trailingOneCount => Some(startBit + trailingOneCount as usize - bitIndexInWord),
        }
    }

    pub fn get(&self, elemIndex: usize) -> bool {
        let (index, bit_index) = Self::wordIndexBitIndexInWord(elemIndex);
        let group = self.words[index];
        group & Bitmap::getSetMask(bit_index) != 0
    }

    /// return true if the bit's group is all set
    pub fn set(&mut self, elemIndex: usize) -> bool {
        let (wordIndex, bitIndexInWord) = Self::wordIndexBitIndexInWord(elemIndex);

        let word = self.words[wordIndex] | Self::getSetMask(bitIndexInWord);
        self.words[wordIndex] = word;

        word == u64::MAX
    }

    pub fn clear(&mut self, elemIndex: usize) {
        let (wordIndex, bitIndex) = Self::wordIndexBitIndexInWord(elemIndex);
        self.words[wordIndex] &= !Self::getSetMask(bitIndex);
    }

    /// 4字节 元素数量
    pub fn serialize(&self) -> Vec<u8> {
        let mut binary = vec![];

        binary.extend(self.elementCount.to_be_bytes());

        for x in &self.words {
            binary.extend(x.to_be_bytes());
        }

        binary
    }

    pub(crate) fn refresh(&self, dest: &mut [u8]) {
        for (i, word) in self.words.iter().enumerate() {
            let wordStartPos = Self::ELEMENT_COUNT_BYTE_LEN + i * Self::WORD_BYTE_LEN;
            let dest = &mut dest[wordStartPos..(wordStartPos + Self::WORD_BYTE_LEN)];
            dest.copy_from_slice(&word.to_be_bytes());
        }
    }

    pub fn deserialize(binary: &[u8]) -> Bitmap {
        let mut words = vec![];

        // 打头的4字节是元素数量
        let elementCount = usize::from_be_bytes(binary[..Self::ELEMENT_COUNT_BYTE_LEN].try_into().unwrap());
        let wordCount = (binary.len() - Self::ELEMENT_COUNT_BYTE_LEN) / Self::WORD_BYTE_LEN;

        for i in 0..wordCount {
            let wordStartPos = Self::ELEMENT_COUNT_BYTE_LEN + i * Self::WORD_BYTE_LEN;
            let word = u64::from_be_bytes(binary[wordStartPos..(wordStartPos + Self::WORD_BYTE_LEN)].try_into().unwrap());
            words.push(word);
        }

        Bitmap {
            elementCount,
            words,
        }
    }
}

pub(crate) struct BitmapIter<'a> {
    len: usize,
    data: &'a [u64],
    data_index: usize,
    current: u64,
}

impl<'a> BitmapIter<'a> {
    fn new(len: usize, data: &'_ [u64]) -> BitmapIter<'_> {
        BitmapIter {
            len,
            data,
            data_index: 0,
            current: data[0],
        }
    }
}

impl Iterator for BitmapIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_index * u64::BITS as usize >= self.len {
            return None;
        }

        if self.current != 0 {
            let mut result = self.data_index;
            result *= u64::BITS as usize;
            let bit = self.current.trailing_zeros() as usize;
            result += bit;
            self.current &= !Bitmap::getSetMask(bit as usize);
            if result >= self.len {
                return None;
            }
            return Some(result);
        }

        self.data_index += 1;
        while self.data_index < self.data.len() {
            let next = self.data[self.data_index];
            if next != 0 {
                self.current = next;
                return self.next();
            }
            self.data_index += 1;
        }

        None
    }
}

pub(crate) struct BitmapDifference<'a, 'b> {
    data: &'a [u64],
    exclusion_data: &'b [u64],
    data_index: usize,
    current: u64,
}

impl<'a, 'b> BitmapDifference<'a, 'b> {
    fn new(data: &'a [u64], exclusion_data: &'b [u64]) -> Self {
        assert_eq!(data.len(), exclusion_data.len());
        let base = data[0];
        let exclusion = exclusion_data[0];
        Self {
            data,
            exclusion_data,
            data_index: 0,
            current: base & (!exclusion),
        }
    }
}

impl Iterator for BitmapDifference<'_, '_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current != 0 {
            let mut result: u32 = self.data_index.try_into().unwrap();
            result *= u64::BITS;
            let bit = self.current.trailing_zeros();
            result += bit;
            self.current &= !Bitmap::getSetMask(bit as usize);
            return Some(result);
        }
        self.data_index += 1;
        while self.data_index < self.data.len() {
            let next = self.data[self.data_index];
            let exclusion = *self.exclusion_data.get(self.data_index).unwrap_or(&0);
            let next = next & (!exclusion);
            if next != 0 {
                self.current = next;
                return self.next();
            }
            self.data_index += 1;
        }
        None
    }
}