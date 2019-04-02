CREATE KEYSPACE IF NOT EXISTS creditcard
WITH replication = {
'class': 'SimpleStrategy', 'replication_factor': 1
};

USE creditcard;

CREATE TABLE IF NOT EXISTS transaction (
  cc_num text,
  transId text,
  transTime timestamp,
  category text,
  merchant text,
  amt double,
  merchLatitude double,
  merchLongitude double,
  PRIMARY KEY(cc_num, transTime)
)WITH CLUSTERING ORDER BY (transTime DESC);