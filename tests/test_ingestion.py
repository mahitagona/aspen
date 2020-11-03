import unittest

from src.entrypoint import parse_xml


class TestIngestion(unittest.TestCase):
    def test_parse_xml(self):
        sample_input_xml = '<xml xmlns:dt="uuid:C2F41010-65B3-11d1-A29F-00AA00C14882" xmlns:rs="urn:schemas-microsoft-com:rowset" xmlns:s="uuid:BDC6E3F0-6DA3-11d1-A2A3-00AA00C14882" xmlns:z="#RowsetSchema"><s:Schema id="RowsetSchema"><s:ElementType content="eltOnly" name="row" rs:updatable="true"><s:AttributeType name="MonthYear" rs:nullable="true" rs:number="1" rs:write="true"><s:datatype dt:maxLength="16" dt:type="dateTime" rs:dbtype="variantdate" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Deposit" rs:nullable="true" rs:number="2" rs:write="true"><s:datatype dt:maxLength="8" dt:type="number" rs:dbtype="currency" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Payment" rs:nullable="true" rs:number="3" rs:write="true"><s:datatype dt:maxLength="8" dt:type="number" rs:dbtype="currency" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Description" rs:nullable="true" rs:number="4" rs:write="true"><s:datatype dt:maxLength="2048" dt:type="string" rs:dbtype="str" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:AttributeType name="Projected" rs:nullable="true" rs:number="5" rs:write="true"><s:datatype dt:maxLength="8" dt:type="number" rs:dbtype="currency" rs:fixedlength="true" rs:maybenull="false" rs:precision="0"/></s:AttributeType><s:extends type="rs:rowbase"/></s:ElementType></s:Schema><rs:data><z:row Description="Starting Balance" Projected="161.71"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-04-01T00:00:00" Payment="1.81" Projected="214.85"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-05-01T00:00:00" Payment="1.81" Projected="267.99"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-06-01T00:00:00" Payment="1.81" Projected="321.13"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-07-01T00:00:00" Payment="1.81" Projected="374.27"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-08-01T00:00:00" Payment="1.81" Projected="427.41"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-09-01T00:00:00" Payment="1.81" Projected="480.55"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-10-01T00:00:00" Payment="1.81" Projected="533.69"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-11-01T00:00:00" Payment="1.81" Projected="586.83"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2019-12-01T00:00:00" Payment="1.81" Projected="639.97"/><z:row Description="Escrowed Tax Payment - County" MonthYear="2019-12-01T00:00:00" Payment="637.64" Projected="2.33"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2020-01-01T00:00:00" Payment="1.81" Projected="55.47"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2020-02-01T00:00:00" Payment="1.81" Projected="108.61"/><z:row Deposit="54.95" Description="Insurance" MonthYear="2020-03-01T00:00:00" Payment="1.81" Projected="161.75"/></rs:data></xml>'
        parsed = parse_xml(sample_input_xml)
        self.assertEqual([[None, 'Starting Balance', None, None, '161.71'],
                          ['54.95', 'Insurance', '2019-04-01T00:00:00', '1.81', '214.85'],
                          ['54.95', 'Insurance', '2019-05-01T00:00:00', '1.81', '267.99'],
                          ['54.95', 'Insurance', '2019-06-01T00:00:00', '1.81', '321.13'],
                          ['54.95', 'Insurance', '2019-07-01T00:00:00', '1.81', '374.27'],
                          ['54.95', 'Insurance', '2019-08-01T00:00:00', '1.81', '427.41'],
                          ['54.95', 'Insurance', '2019-09-01T00:00:00', '1.81', '480.55'],
                          ['54.95', 'Insurance', '2019-10-01T00:00:00', '1.81', '533.69'],
                          ['54.95', 'Insurance', '2019-11-01T00:00:00', '1.81', '586.83'],
                          ['54.95', 'Insurance', '2019-12-01T00:00:00', '1.81', '639.97'],
                          [None, 'Escrowed Tax Payment - County', '2019-12-01T00:00:00', '637.64', '2.33'],
                          ['54.95', 'Insurance', '2020-01-01T00:00:00', '1.81', '55.47'],
                          ['54.95', 'Insurance', '2020-02-01T00:00:00', '1.81', '108.61'],
                          ['54.95', 'Insurance', '2020-03-01T00:00:00', '1.81', '161.75']]
                         , parsed)

