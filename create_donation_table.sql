CREATE TABLE Donation
(Don_ID NUMBER PRIMARY KEY,
Donor_First_name VARCHAR2(16),
Donor_Last_name VARCHAR2(16),
Donation_Date Date NOT NULL,
Donation_Amount NUMBER(7,1) NOT NULL,
Address_ID  NOT NULL,
Volunteer_ID NOT NULL,
CONSTRAINT fk_don_add
FOREIGN KEY (Address_ID) REFERENCES Address(Address_ID),
CONSTRAINT fk_don_vol
FOREIGN KEY (Volunteer_ID) REFERENCES Volunteer(Volunteer_ID));