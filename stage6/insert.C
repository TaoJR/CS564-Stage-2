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
    Status status;
    RelDesc rd;
    AttrDesc *attrs = NULL;
    int relAttrCnt;

    if (relation.empty())
        return BADCATPARM;

    if ((status = relCat->getInfo(relation, rd)) != OK)
        return status;

    if (attrCnt != rd.attrCnt)
        return ATTRNOTFOUND;

    if ((status = attrCat->getRelInfo(rd.relName, relAttrCnt, attrs)) != OK)
        return status;

    int recLen = 0;
    for (int i = 0; i < relAttrCnt; i++)
        recLen += attrs[i].attrLen;

    char *recData = new char[recLen];
    if (!recData) {
        free(attrs);
        return INSUFMEM;
    }
    memset(recData, 0, recLen);

    for (int i = 0; i < relAttrCnt; i++) {
        int j;
        for (j = 0; j < attrCnt; j++) {
            if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0)
                break;
        }
        if (j == attrCnt) {
            delete [] recData;
            free(attrs);
            return ATTRNOTFOUND;
        }

        if (attrList[j].attrValue == NULL) {
            delete [] recData;
            free(attrs);
            return ATTRNOTFOUND;
        }

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

    InsertFileScan *ifs = new InsertFileScan(relation, status);
    if (status != OK) {
        delete [] recData;
        free(attrs);
        return status;
    }

    Record rec;
    rec.data = recData;
    rec.length = recLen;

    RID rid;
    status = ifs->insertRecord(rec, rid);

    delete ifs;
    delete [] recData;
    free(attrs);

    return status;
}

