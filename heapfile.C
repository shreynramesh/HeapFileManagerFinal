#include "error.h"
#include "heapfile.h"

// TODO: routine to create a heapfile
const Status createHeapFile(const string fileName) {
    File* file;
    Status status;
    FileHdrPage* hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page* newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK) {
        // create and open the file
        status = db.createFile(fileName);
        if (status != OK) return (status);
        status = db.openFile(fileName, file);
        if (status != OK) return (status);

        // allocate header and data pages
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) return (status);
        FileHdrPage* hdrPage = (FileHdrPage*)newPage;
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) return (status);

        // initialize header and data pages
        newPage->init(newPageNo);
        int len_name = fileName.size();  // there is probably a better way to do string assignment
        for (int i = 0; i != len_name; ++i) {
            hdrPage->fileName[i] = fileName[i];
        }
        hdrPage->fileName[len_name] = '\0';
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // unpin header and data pages, set dirty bit to true
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) return (status);
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) return (status);

        // flush and close file
        status = bufMgr->flushFile(file);
        if (status != OK) return (status);
        status = db.closeFile(file);
        if (status != OK) return (status);

        return (OK);
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName) {
    return (db.destroyFile(fileName));
}

// TODO: constructor opens the underlying file
HeapFile::HeapFile(const string& fileName, Status& returnStatus) {
    Status status;
    Page* pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK) {
        // assign the header page
        status = filePtr->getFirstPage(headerPageNo);
        returnStatus = status;
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        returnStatus = status;
        headerPage = (FileHdrPage*)pagePtr;
        hdrDirtyFlag = false;

        // read the first data page into curPage and curPageNo
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        returnStatus = status;

        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = OK;
        return;
    } else {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile() {
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK) {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file
const int HeapFile::getRecCnt() const {
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

// TODO
const Status HeapFile::getRecord(const RID& rid, Record& rec) {
    Status status;
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    // If on correct page: curPageNo == rid.pageNo
    if (curPageNo == rid.pageNo) {
        // Call getRecord on current page (gets record by slot number)
        status = curPage->getRecord(rid, rec);
        // Update HeapFile object
        curRec = rid;
        return status;
    } else {
        // unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            // update heapfile object to reflect
            curPage = NULL;
            curPageNo = 0;
            curDirtyFlag = false;
            return status;
        }
    }

    // Read page using curPageNo, then call getRecord
    status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if (status != OK) return status;
    // update heapfile object
    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    curRec = rid;

    // get the record
    return curPage->getRecord(rid, rec);
}

HeapFileScan::HeapFileScan(const string& name,
                           Status& status) : HeapFile(name, status) {
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char* filter_,
                                     const Operator op_) {
    if (!filter_) {  // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE)) {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan() {
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan() {
    endScan();
}

const Status HeapFileScan::markScan() {
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan() {
    Status status;
    if (markedPageNo != curPageNo) {
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;  // it will be clean
    } else
        curRec = markedRec;
    return OK;
}

// TODO
const Status HeapFileScan::scanNext(RID& outRid) {
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    Record rec;
    int nextPageNo;
    bool matchFound = false;

    // Check if curPage is NULL and handle it
    if (curPage == NULL) {
        return BADPAGEPTR;
    }

    if (curPageNo < 0) {
        return FILEEOF;
    }

    // Start search from next record after curRec and advance page to first one with records if we are at ENDOFPAGE
    status = curPage->nextRecord(curRec, tmpRid);
    if (status == ENDOFPAGE) {
        // Finding next non-empty page
        do {
            status = curPage->getNextPage(nextPageNo);
            if (nextPageNo == -1) {
                return FILEEOF;
            }
            if (status != OK) {
                return status;
            }

            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                return status;
            }
            curPage = NULL;
            curPageNo = -1;

            curDirtyFlag = false;
            curPageNo = nextPageNo;
            // curPage->getNextPage(nextPageNo);

            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) {
                return status;
            }

        } while ((status = curPage->firstRecord(tmpRid)) == NORECORDS);
    } else if (status != OK) {
        return status;
    }

    // Loop through each page until match is found
    while (true) {
        // cout << "New Loop: ";
        // Loop through each record on the page
        while (true) {
            // Check if there is a match in the current record
            curPage->getRecord(tmpRid, rec);
            if (matchRec(rec)) {
                // cout << "Page Number" << tmpRid.pageNo << endl;
                // cout << "Slot Number: " << tmpRid.slotNo << endl;

                matchFound = true;
                curRec = tmpRid;
                outRid = curRec;
                break;  // Break out of the inner loop
            }

            // Get the next RID on the page
            if (curPage->nextRecord(tmpRid, nextRid) == ENDOFPAGE) {
                // cout << "END OF PAGE: " << curPageNo << " " << nextPageNo << " ";
                curPage->getNextPage(nextPageNo);

                // Unpin the current page
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                curDirtyFlag = false;

                // Move to the next page if valid
                if (nextPageNo != -1) {
                    curPageNo = nextPageNo;
                    status = bufMgr->readPage(filePtr, curPageNo, curPage);

                    if (status != OK) {
                        return status;
                    }

                    status = curPage->firstRecord(tmpRid);
                    if (status != OK) {
                        return status;
                    }
                    // cout << "END OF PAGE AFTER INC: " << curPageNo << " " << nextPageNo << " " << endl;
                }

                break;  // Break out of the inner loop if end of page is reached
            } else {
                tmpRid = nextRid;
            }
        }

        // If match found or end of file, break out of the outer loop
        if (matchFound || nextPageNo == -1) {
            break;
        }
    }

    // Return OK if match found, otherwise return error
    return (matchFound) ? OK : FILEEOF;
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page
const Status HeapFileScan::getRecord(Record& rec) {
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord() {
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty() {
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record& rec) const {
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0;  // < 0 if attr < fltr
    switch (type) {
        case INTEGER:
            int iattr, ifltr;  // word-alignment problem possible
            memcpy(&iattr,
                   (char*)rec.data + offset,
                   length);
            memcpy(&ifltr,
                   filter,
                   length);
            diff = iattr - ifltr;
            break;

        case FLOAT:
            float fattr, ffltr;  // word-alignment problem possible
            memcpy(&fattr,
                   (char*)rec.data + offset,
                   length);
            memcpy(&ffltr,
                   filter,
                   length);
            diff = fattr - ffltr;
            break;

        case STRING:
            diff = strncmp((char*)rec.data + offset,
                           filter,
                           length);
            break;
    }

    switch (op) {
        case LT:
            if (diff < 0.0) return true;
            break;
        case LTE:
            if (diff <= 0.0) return true;
            break;
        case EQ:
            if (diff == 0.0) return true;
            break;
        case GTE:
            if (diff >= 0.0) return true;
            break;
        case GT:
            if (diff > 0.0) return true;
            break;
        case NE:
            if (diff != 0.0) return true;
            break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string& name,
                               Status& status) : HeapFile(name, status) {
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan() {
    Status status;
    // unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// TODO: Insert a record into the file
const Status InsertFileScan::insertRecord(const Record& rec, RID& outRid) {
    Page* newPage;
    int newPageNo;
    Status status, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED) {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // check if current page is valid
    if (curPage != NULL) {
        // call insert record on current page
        status = curPage->insertRecord(rec, rid);
        if (status == OK) {
            // do bookkeeping: update header page and current page metadata
            headerPage->recCnt++;
            hdrDirtyFlag = true;
            curDirtyFlag = true;
            outRid = rid;
            return status;
        } else {
            // allocate a new page
            status = bufMgr->allocPage(filePtr, newPageNo, newPage);
            if (status != OK) return status;
            // initialize page
            newPage->init(newPageNo);
            status = newPage->setNextPage(-1);
            // bookkeeping: update header page metadata
            headerPage->lastPage = newPageNo;
            headerPage->pageCnt++;
            hdrDirtyFlag = true;
            // link current page to new page
            status = curPage->setNextPage(newPageNo);
            if (status != OK) return status;
            // unpin current page
            status = bufMgr->unPinPage(filePtr, curPageNo, true);
            if (status != OK) {
                // bookkeeping: update current page metadata
                curDirtyFlag = false;
                curPage = NULL;
                curPageNo = -1;
                // unpin the newly added page
                unpinstatus = bufMgr->unPinPage(filePtr, newPageNo, true);
                return status;
            }
            // update current page to new page
            curPageNo = newPageNo;
            curPage = newPage;
            // call insertRecord on new page
            status = curPage->insertRecord(rec, rid);
            if (status == OK) {
                // bookkeeping
                outRid = rid;
                curDirtyFlag = true;
                hdrDirtyFlag = true;
                headerPage->recCnt++;
                return status;
            }
            return status;
        }
    } else {
        // set last page as current page
        curPageNo = headerPage->lastPage;
        // read the last page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
    }
}