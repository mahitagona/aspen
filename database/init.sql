CREATE TABLE Projections (
	ProjectionID INTEGER PRIMARY KEY AUTOINCREMENT,
  StartDate varchar ,
	XML text
);

--Example
INSERT INTO Projections (StartDate, XML) VALUES
('2019-04-01', '<xml xmlns:dt="uuid:C2F41010-65B3-11d1-A29F-00AA00C14882" xmlns:rs="urn:schemas-microsoft-com:rowset" xmlns:s="uuid:BDC6E3F0-6DA3-11d1-A2A3-00AA00C14882" xmlns:z="#RowsetSchema"><s:Schema id="RowsetSchema"><s:ElementType content="eltOnly" name="row" rs:updatable="true"><s:AttributeType name="MonthYear" rs:nullable="true" rs:number="1" rs:write="true"><s:datatype dt:maxLength="16" dt:type="dateTime" rs:dbtype="variantdate" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Deposit" rs:nullable="true" rs:number="2" rs:write="true"><s:datatype dt:maxLength="8" dt:type="number" rs:dbtype="currency" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Payment" rs:nullable="true" rs:number="3" rs:write="true"><s:datatype dt:maxLength="8" dt:type="number" rs:dbtype="currency" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Description" rs:nullable="true" rs:number="4" rs:write="true"><s:datatype dt:maxLength="2048" dt:type="string" rs:dbtype="str" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Projected" rs:nullable="true" rs:number="5" rs:write="true"><s:datatype dt:maxLength="8" dt:type="number" rs:dbtype="currency" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:extends type="rs:rowbase"/></s:ElementType></s:Schema><rs:data><z:row Description="Starting Balance" Projected="161.71"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-04-01T00:00:00" Payment="1.81" Projected="214.85"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-05-01T00:00:00" Payment="1.81" Projected="267.99"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-06-01T00:00:00" Payment="1.81" Projected="321.13"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-07-01T00:00:00" Payment="1.81" Projected="374.27"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-08-01T00:00:00" Payment="1.81" Projected="427.41"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-09-01T00:00:00" Payment="1.81" Projected="480.55"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-10-01T00:00:00" Payment="1.81" Projected="533.69"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-11-01T00:00:00" Payment="1.81" Projected="586.83"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-12-01T00:00:00" Payment="1.81" Projected="639.97"/><z:row Description="Escrowed Tax Payment - County" MonthYear="2019-12-01T00:00:00" Payment="637.64" Projected="2.33"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2020-01-01T00:00:00" Payment="1.81" Projected="55.47"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2020-02-01T00:00:00" Payment="1.81" Projected="108.61"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2020-03-01T00:00:00" Payment="1.81" Projected="161.75"/></rs:data></xml>');
;

CREATE TABLE Actuals (
  ActualID INTEGER PRIMARY KEY AUTOINCREMENT,
  Amount DECIMAL(13,2) NOT NULL,
  Memo text,
  DateDeposited varchar NOT NULL,
  ProjectionID INTEGER NOT NULL REFERENCES Projections(ProjectionID)
);

--Example
INSERT INTO Actuals (Amount, Memo, DateDeposited, ProjectionID) VALUES
  (-2.04, 'Insurance', '2019-04-01', 1),
  (-2.27, 'Insurance', '2019-05-01', 1),
  (-2.18, 'Insurance', '2019-06-01', 1),
  (-2.26, 'Insurance', '2019-07-01', 1),
  (-9.38, 'Insurance', '2019-08-01', 1),
  (-9.38, 'Insurance', '2019-08-01', 1),
  (-9.38, 'Insurance', '2019-08-01', 1)
  --etc
;
