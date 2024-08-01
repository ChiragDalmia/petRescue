CREATE TABLE Address(
Address_ID NUMBER PRIMARY KEY,
Unit_Num VARCHAR2(6),
Street_Number NUMBER NOT NULL,
Street_Name VARCHAR2(24) NOT NULL,
Street_Type VARCHAR2(12) NOT NULL,
Street_Direction CHAR(1),
Postal_Code CHAR(7) NOT NULL,
City VARCHAR2(16) NOT NULL,
Province CHAR(2) NOT NULL,
CONSTRAINT St_Dir CHECK
(Street_Direction IN ('E', 'W', 'N', 'S')));