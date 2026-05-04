#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{

	cout << "Doing QU_Delete " << endl;

    Status status;
    RID    rid;

    // open heap scan on target relation
    HeapFileScan heapScan(relation, status);
    if (status != OK) return status;

    if (attrName.empty())
    {
        // no predicate: scan entire file
        status = heapScan.startScan(0, 0, STRING, NULL, EQ);
    }
    else
    {
        AttrDesc attrDesc;
        // lookup predicate column
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) return status;

        // parser type must match catalog
        if (type != attrDesc.attrType)
            return ATTRTYPEMISMATCH;

        int   filterInt;
        float filterFloat;
        const char *filterPtr = NULL;

        // build binary filter from literal
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

        // start filtered scan
        status = heapScan.startScan(attrDesc.attrOffset,
                                    attrDesc.attrLen,
                                    (Datatype) attrDesc.attrType,
                                    filterPtr,
                                    op);
    }
    if (status != OK) return status;

    // delete each matching tuple
    while ((status = heapScan.scanNext(rid)) == OK)
    {
        status = heapScan.deleteRecord();
        if (status != OK) return status;
    }

    // expect EOF after last tuple
    if (status != FILEEOF)
        return status;

    return OK;



}

