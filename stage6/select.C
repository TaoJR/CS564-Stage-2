#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"


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
    cout << "Doing QU_Select " << endl;

    Status status;

    // resolve projection columns from catalog
    AttrDesc projDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName,
                                  projNames[i].attrName,
                                  projDescArray[i]);
        if (status != OK) return status;
    }

    // sum attribute lengths for output row
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
        reclen += projDescArray[i].attrLen;

    // no WHERE: copy all rows
    if (attr == NULL)
    {
        return ScanSelect(result,
                          projCnt,
                          projDescArray,
                          NULL,
                          EQ,
                          NULL,
                          reclen);
    }

    AttrDesc attrDesc;
    // lookup column used in predicate
    status = attrCat->getInfo(attr->relName,
                              attr->attrName,
                              attrDesc);
    if (status != OK) return status;

    // parser type must match catalog
    if (attr->attrType != attrDesc.attrType)
        return ATTRTYPEMISMATCH;

    int   filterInt;
    float filterFloat;
    const char *filterPtr = NULL;

    // literal string to typed filter bytes
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

    // run heap scan and materialize result
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

    // open result relation for inserts
    InsertFileScan resultRel(result, status);
    if (status != OK) return status;

    // open scan on source relation
    HeapFileScan heapScan(string(projNames[0].relName), status);
    if (status != OK) return status;

    if (attrDesc == NULL)
    {
        // scan all tuples
        status = heapScan.startScan(0, 0, STRING, NULL, EQ);
    }
    else
    {
        // scan tuples matching predicate
        status = heapScan.startScan(attrDesc->attrOffset,
                                    attrDesc->attrLen,
                                    (Datatype) attrDesc->attrType,
                                    filter,
                                    op);
    }
    if (status != OK) return status;

    // one output buffer reused per row
    char outputData[reclen];
    Record outputRec;
    outputRec.data   = (void *) outputData;
    outputRec.length = reclen;

    RID    rid;
    Record rec;

    // for each hit: project then append
    while ((status = heapScan.scanNext(rid)) == OK)
    {
        // read current input tuple
        status = heapScan.getRecord(rec);
        if (status != OK) return status;

        // pack projected columns into output buffer
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            memcpy(outputData + outputOffset,
                   (char *) rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }

        // write row to result file
        RID outRid;
        status = resultRel.insertRecord(outputRec, outRid);
        if (status != OK) return status;
        resultTupCnt++;
    }

    // normal end is FILEEOF
    if (status != FILEEOF)
        return status;

    return OK;
}
