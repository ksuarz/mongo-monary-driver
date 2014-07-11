#ifndef CMONARYMODULE_H
#define CMONARYMODULE_H

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

/*
 *-------------------------------------------------------------------------
 * cmonary_t --
 *
 *      Represents a "Monary" connection.
 *
 *  Members:
 *      @coldata: An array of Python lists.
 *      @client: A mongoc_client_t.
 *-------------------------------------------------------------------------
 */
typedef struct {
    PyObject_HEAD
    PyListObject   **coldata;
    mongoc_client_t *client;
} cmonary_t;

/*
 *-------------------------------------------------------------------------
 * cmonary_list_t --
 *
 *      Represents a Monary list, which holds a column of data retrieved from
 *      MongoDB.
 *
 *  Members:
 *      @list: A Python list.
 *      @field: The name of the field to which this list corresponds.
 *-------------------------------------------------------------------------
 */
typedef struct {
    PyListObject *list;
    char         *field;
} cmonary_list_t;

/*
 *-------------------------------------------------------------------------
 * cmonary_members --
 *
 *      An array of the members of our type.
 *
 *  Members:
 *      @coldata: A pointer to a Python list.
 *      @client: A pointer to a mongoc_client_t.
 *-------------------------------------------------------------------------
 */
static PyMemberDef cmonary_members[] = {
    {
        "coldata",
        T_OBJECT_EX,
        offsetof(cmonary_t, coldata),
        0,
        "Column data"
    },
    {
        "client",
        T_OBJECT_EX,
        offsetof(cmonary_t, client),
        0,
        "mongoc_client_t"
    },
    {
        NULL /* Sentinel */
    }
}

/* Function prototypes */
int _cmonary_pylist_contains_string (PyListObject *, char *);
int _cmonary_load_cursor (PyObject ***, PyListObject *, mongoc_cursor_t *);
int _cmonary_load_cursor_single (PyObject ***, const char *, mongoc_cursor_t *);

#endif
