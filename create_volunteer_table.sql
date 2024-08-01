CREATE TABLE Volunteer(
Volunteer_ID NUMBER PRIMARY KEY,
First_name VARCHAR2(16),
Last_name VARCHAR2(16),
Group_Leader,
CONSTRAINT fk_vol_lead
FOREIGN KEY (Group_Leader)
REFERENCES Volunteer(Volunteer_ID)
);