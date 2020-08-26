import datetime
import cx_Oracle

# Mocked SQL responses for tests
table_names = {"data": [("TEST_TABLE1",), ("TEST_TABLE2",), ("SYS_TABLE",)]}
primary_key = {"data": [("LONG_POSTCODE_ID",)]}
primary_keys = {"data": [("LONG_POSTCODE_ID",), ("TEAM_ID",)]}
partition = {"data": [("P_ADDITIONAL_IDENTIFIER",)]}
partitions = {
    "data": [
        ("P_ADDITIONAL_IDENTIFIER",),
        ("P_ADDITIONAL_OFFENCE",),
        ("P_ADDITIONAL_SENTENCE",),
        ("P_ADDRESS",),
        ("P_ADDRESS_ASSESSMENT",),
        ("P_ALIAS",),
        ("P_APPROVED_PREMISES_REFERRAL",),
    ]
}
subpartition = {"data": [("SUBPARTITION_A",)]}
subpartitions = {
    "data": [
        ("SUBPARTITION_A",),
        ("SUBPARTITION_B",),
        ("SUBPARTITION_C",),
        ("SUBPARTITION_D",),
    ]
}
first_table = {
    "desc": [
        ("TEST_NUMBER", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
        ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
        ("TEST_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
        ("TEST_VARCHAR", cx_Oracle.DB_TYPE_VARCHAR, 30, 30, None, None, 1),
        ("TEST_FLAG", cx_Oracle.DB_TYPE_VARCHAR, 1, 1, None, None, 1),
        ("TEST_ROWID_SKIP", cx_Oracle.DB_TYPE_ROWID, 127, None, 0, -127, 1),
        ("TEST_OBJECT_SKIP", cx_Oracle.DB_TYPE_OBJECT, 127, None, 0, -127, 1),
    ],
    "data": [
        (
            63495,
            7833,
            datetime.datetime(2020, 6, 23, 10, 39, 12),
            "INSTITUTIONAL_REPORT_TRANSFER",
            "I",
            12345678,
            "OBJECT",
        )
    ],
}
second_table = {
    "desc": [
        ("SPG_ERROR_ID", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
        ("ERROR_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
        ("MESSAGE_CRN", cx_Oracle.DB_TYPE_CHAR, 7, 7, None, None, 1),
        ("NOTES", cx_Oracle.DB_TYPE_CLOB, None, None, None, None, 1),
        ("INCIDENT_ID", cx_Oracle.DB_TYPE_VARCHAR, 100, 100, None, None, 1),
    ],
    "data": [
        (
            198984,
            datetime.datetime(2018, 8, 2, 14, 49, 21),
            "E160306",
            "CLOB TEXT",
            1500148234,
        )
    ],
}
empty_table = {
    "description": [
        ("TEST_NUMBER", cx_Oracle.DB_TYPE_NUMBER, 39, None, 38, 0, 1),
        ("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1),
        ("TEST_DATE", cx_Oracle.DB_TYPE_DATE, 23, None, None, None, 1),
        ("TEST_VARCHAR", cx_Oracle.DB_TYPE_VARCHAR, 30, 30, None, None, 1),
        ("TEST_FLAG", cx_Oracle.DB_TYPE_VARCHAR, 1, 1, None, None, 1),
        ("TEST_ROWID_SKIP", cx_Oracle.DB_TYPE_ROWID, 127, None, 0, -127, 1),
        ("TEST_OBJECT_SKIP", cx_Oracle.DB_TYPE_OBJECT, 127, None, 0, -127, 1),
    ]
}
doc_history = {
    "desc": [("TEST_ID", cx_Oracle.DB_TYPE_NUMBER, 127, None, 0, -127, 1)],
}
