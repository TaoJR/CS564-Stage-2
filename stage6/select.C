#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
    // QU_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    Status status;

    // ---- 1. resolve every projection-list attribute via the attrCat ---------
    AttrDesc projDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName,
                                  projNames[i].attrName,
                                  projDescArray[i]);
        if (status != OK) return status;
    }

    // ---- 2. compute the output record length --------------------------------
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
        reclen += projDescArray[i].attrLen;

    // ---- 3. unconditional scan: attr == NULL --------------------------------
    if (attr == NULL)
    {
        return ScanSelect(result,
                          projCnt,
                          projDescArray,
                          NULL,        // no filter attribute
                          EQ,          // op is irrelevant
                          NULL,        // no filter value
                          reclen);
    }

    // ---- 4. otherwise look up the predicate attribute -----------------------
    AttrDesc attrDesc;
    status = attrCat->getInfo(attr->relName,
                              attr->attrName,
                              attrDesc);
    if (status != OK) return status;

    // sanity check: parser-supplied type should match catalog
    if (attr->attrType != attrDesc.attrType)
        return ATTRTYPEMISMATCH;

    // ---- 5. convert the string-form filter value into a binary buffer -------
    // The filter buffer must outlive the call to ScanSelect/startScan, so we
    // declare it on the stack here in this function's frame.
    int   filterInt;
    float filterFloat;
    const char *filterPtr = NULL;

    switch (attrDesc.attrType)
    {
        case INTEGER:
            filterInt = atoi(attrValue);
            filterPtr = (const char *) &filterInt;
            break;
        case FLOAT:
            filterFloat = (float) atof(attrValue);
            filterPtr = (const char *) &filterFloat;
            break;
        case STRING:
            filterPtr = attrValue;
            break;
        default:
            return ATTRTYPEMISMATCH;
    }

    // ---- 6. hand off to ScanSelect ------------------------------------------
    return ScanSelect(result,
                      projCnt,
                      projDescArray,
                      &attrDesc,
                      op,
                      filterPtr,
                      reclen);
}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;
    int    resultTupCnt = 0;

    // ---- 1. open the result relation for inserts ---------------------------
    InsertFileScan resultRel(result, status);
    if (status != OK) return status;

    // ---- 2. open a scan on the input relation -------------------------------
    // The input relation is whichever table the projection columns came from
    // (they all come from the same relation in a single-table SELECT).
    HeapFileScan hfs(string(projNames[0].relName), status);
    if (status != OK) return status;

    if (attrDesc == NULL)
    {
        // unconditional scan: filter == NULL
        status = hfs.startScan(0, 0, STRING, NULL, EQ);
    }
    else
    {
        status = hfs.startScan(attrDesc->attrOffset,
                               attrDesc->attrLen,
                               (Datatype) attrDesc->attrType,
                               filter,
                               op);
    }
    if (status != OK) return status;

    // ---- 3. scan, project on-the-fly, append to result ---------------------
    char outputData[reclen];
    Record outputRec;
    outputRec.data   = (void *) outputData;
    outputRec.length = reclen;

    RID    rid;
    Record rec;

    while ((status = hfs.scanNext(rid)) == OK)
    {
        status = hfs.getRecord(rec);
        if (status != OK) return status;

        // copy each projected attribute into outputData at its packed offset
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            memcpy(outputData + outputOffset,
                   (char *) rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }

        // append to the result relation
        RID outRid;
        status = resultRel.insertRecord(outputRec, outRid);
        if (status != OK) return status;
        resultTupCnt++;
    }

    // FILEEOF means we scanned everything successfully
    if (status != FILEEOF)
        return status;

    // endScan / heapfile cleanup happen via destructors when hfs/resultRel
    // go out of scope.
    return OK;
}