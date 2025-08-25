/*
 * Copyright (c) 2024-2025 fenquen(https://github.com/fenquen), licensed under Apache 2.0
 */

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use crate::page::Page;
use crate::types::PageId;

pub(crate) struct LruCache {
    pub(crate) capacity: usize,

    pub(crate) map: HashMap<PageId, Arc<RwLock<Page>>>,

    // 越往后的元素是越最近接触过的
    pub(crate) leastRecentPageIds: VecDeque<PageId>,
}

impl LruCache {
    pub(crate) fn new(capacity: usize) -> LruCache {
        LruCache {
            capacity,
            map: HashMap::with_capacity(capacity),
            leastRecentPageIds: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn get(&self, pageId: PageId) -> Option<Arc<RwLock<Page>>> {
        if let Some(page) = self.map.get(&pageId) {
            //visited.store(true, Ordering::Release);
            Some(page.clone())
        } else {
            None
        }
    }

    pub(crate) fn set(&mut self, pageId: PageId, page: Arc<RwLock<Page>>) {
        if let None = self.map.insert(pageId, page) {
            self.leastRecentPageIds.push_back(pageId);
        }

        if self.map.len() > self.capacity {
            for _ in 0..self.leastRecentPageIds.len() {
                if let Some(pageId) = self.leastRecentPageIds.pop_front() {
                    if let Some(page) = self.map.get(&pageId) {
                        if Arc::strong_count(page) > 1 {
                            self.leastRecentPageIds.push_back(pageId);
                        } else {
                            self.map.remove(&pageId);
                        }
                    }
                }
            }
        }
    }
}