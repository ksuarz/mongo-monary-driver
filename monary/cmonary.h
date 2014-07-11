#ifndef CMONARYMODULE_H
#define CMONARYMODULE_H

#include <Python2.7/Python.h>
#include <mongoc.h>
#include <bson.h>
#include <bson.h>

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
 *      @client: A mongoc_client_t.
 *      @columns: A Python list of Python lists, where each sublist represents a
 *                column of data extracted from MongoDB.
 *      @fieldnames: The field names corresponding to the column lists.
 *-------------------------------------------------------------------------
 */
typedef struct {
    PyObject_HEAD
    mongoc_client_t *client;
} cmonary_t;

typedef struct {
    PyListObject *columns;
    char        **fieldnames;
} cmonary_column_t;

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
        "columns",
        T_OBJECT_EX,
        offsetof(cmonary_t, columns),
        0,
        "Column data"
    },
    {
        "fieldnames",
        T_OBJECT_EX,
        offsetof(cmonary_t, fieldnames);
        0,
        "Column field names"
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

/*
 *-------------------------------------------------------------------------
 * cmonary_methods --
 *
 *      An array of the methods of our type.
 *-------------------------------------------------------------------------
 */
static PyMethodDef cmonary_methods[] = {
    {
        "collection_find_demo",
        (PyCFunction) cmonary_collection_find_demo,
        METH_NOARGS,
        "Does a pre-written sample query."
    },
    {
        NULL /* Sentinel */
    }
};

/*
 *-------------------------------------------------------------------------
 * cmonary_type_t --
 *
 *      Represents the Python type for Monary.
 *-------------------------------------------------------------------------
 */
static PyTypeObject monary_type_t = {
    PyObject_HEAD_INIT(NULL)
    0,                                        /* ob_size*/
    "cmonary.cmonary",                        /* tp_name*/
    sizeof(cmonary_t),                        /* tp_basicsize*/
    0,                                        /* tp_itemsize*/
    (destructor) cmonary_dealloc,             /* tp_dealloc*/
    0,                                        /* tp_print*/
    0,                                        /* tp_getattr*/
    0,                                        /* tp_setattr*/
    0,                                        /* tp_compare*/
    0,                                        /* tp_repr*/
    0,                                        /* tp_as_number*/
    0,                                        /* tp_as_sequence*/
    0,                                        /* tp_as_mapping*/
    0,                                        /* tp_hash */
    0,                                        /* tp_call*/
    0,                                        /* tp_str*/
    0,                                        /* tp_getattro*/
    0,                                        /* tp_setattro*/
    0,                                        /* tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags*/
    "Monary C driver.",                       /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    cmonary_methods,                          /* tp_methods */
    cmonary_members,                          /* tp_members */
    0,                                        /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    (initproc) cmonary_init,                  /* tp_init */
    0,                                        /* tp_alloc */
    cmonary_new,                              /* tp_new */
};

/* Function prototypes */
static void cmonary_dealloc (cmonary_t *);
static void cmonary_dealloc_columns (cmonary_column_t *);
static PyObject *cmonary_new (PyTypeObject *, PyObject *);
static int cmonary_init (cmonary_t *, PyObject *, PyObject *);
static PyObject *cmonary_collection_count(cmonary_t *);
static PyObject *cmonary_collection_find_demo(cmonary_t *self);
static int cmonary_load_cursor_demo(mongoc_cursor_t *, PyObject *);
static int _cmonary_pylist_contains_string (PyListObject *, char *);
static int _cmonary_load_cursor (cmonary_list_t *, const char *, mongoc_cursor_t *);
static int _cmonary_load_cursor_single (PyObject ***, const char *, mongoc_cursor_t *);
static int _cmonary_load_item (bson_iter_t *iter, cmonary_list_t *);
static PyObject *cmonary_collection_find(cmonary_t *self, PyObject *, PyObject *);
PyMODINIT_FUNC cmonary_init (void);

#endif
