#include "catalog.h"
#include "query.h"
#include <stdio.h>
#include <stdlib.h>


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation,
	const int attrCnt,
	const attrInfo attrList[])
{
	cout << "Doing QU_Insert " << endl;

    Status status;
    RelDesc rd;
    AttrDesc *attrs = NULL;
    int relAttrCnt;

    // reject empty relation name
    if (relation.empty())
        return BADCATPARM;

    // read relation entry from relcat
    if ((status = relCat->getInfo(relation, rd)) != OK)
        return status;

    // INSERT must supply every attribute
    if (attrCnt != rd.attrCnt)
        return ATTRNOTFOUND;

    // load full attribute metadata for this rel
    if ((status = attrCat->getRelInfo(rd.relName, relAttrCnt, attrs)) != OK)
        return status;

    // total record size from catalog
    int recLen = 0;
    for (int i = 0; i < relAttrCnt; i++)
        recLen += attrs[i].attrLen;

    // allocate tuple buffer
    char *recData = new char[recLen];
    if (!recData) {
        free(attrs);
        return INSUFMEM;
    }
    // zero-fill (pads strings)
    memset(recData, 0, recLen);

    // fill buffer in catalog column order
    for (int i = 0; i < relAttrCnt; i++) {
        int j;
        // find value for this catalog attribute
        for (j = 0; j < attrCnt; j++) {
            if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0)
                break;
        }
        // attribute missing from INSERT list
        if (j == attrCnt) {
            delete [] recData;
            free(attrs);
            return ATTRNOTFOUND;
        }

        // NULL value not allowed
        if (attrList[j].attrValue == NULL) {
            delete [] recData;
            free(attrs);
            return ATTRNOTFOUND;
        }

        // copy typed value to correct offset
        switch (attrs[i].attrType) {
            case INTEGER: {
                int ival = atoi((char *)attrList[j].attrValue);
                memcpy(recData + attrs[i].attrOffset, &ival, attrs[i].attrLen);
                break;
            }
            case FLOAT: {
                float fval = atof((char *)attrList[j].attrValue);
                memcpy(recData + attrs[i].attrOffset, &fval, attrs[i].attrLen);
                break;
            }
            case STRING: {
                memset(recData + attrs[i].attrOffset, 0, attrs[i].attrLen);
                strncpy(recData + attrs[i].attrOffset,
                        (char *)attrList[j].attrValue,
                        attrs[i].attrLen);
                break;
            }
            default:
                delete [] recData;
                free(attrs);
                return ATTRTYPEMISMATCH;
        }
    }

    // open heap for append
    InsertFileScan *ifs = new InsertFileScan(relation, status);
    if (status != OK) {
        delete [] recData;
        free(attrs);
        return status;
    }

    // build Record wrapper
    Record rec;
    rec.data = recData;
    rec.length = recLen;

    RID rid;
    // write page slot
    status = ifs->insertRecord(rec, rid);

    delete ifs;
    delete [] recData;
    free(attrs);

    return status;
}
