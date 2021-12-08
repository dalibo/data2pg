-- tests_pg_setup.sql: Create and setup all application objects that will be needed for the tests
--
SET client_min_messages TO WARNING;

------------------------------------------------------------
-- create several application schemas with tables, sequences, triggers
------------------------------------------------------------
--
-- First schema
--
DROP SCHEMA IF EXISTS mySchema1 CASCADE;
CREATE SCHEMA mySchema1;

SET search_path=mySchema1;

DROP TABLE IF EXISTS myTbl1 ;
CREATE TABLE myTbl1 (
  badName     DECIMAL (7)      NOT NULL,
  col12       CHAR (10)        NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (badName,col12)
);
ALTER TABLE myTbl1 RENAME badName TO col11;
CREATE INDEX myTbl1_idx on myTbl1 (col13);

DROP TABLE IF EXISTS myTbl2 ;
CREATE TABLE myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);

DROP TABLE IF EXISTS "myTbl3" ;
CREATE TABLE "myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32 desc, col33);
ALTER TABLE "myTbl3" CLUSTER ON myIdx3;

DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21) DEFERRABLE INITIALLY IMMEDIATE,
  CONSTRAINT mytbl4_col44_fkey FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE myTbl2b (
      col20       INT              NOT NULL GENERATED ALWAYS AS IDENTITY,
      col21       INT              NOT NULL,
      col22       FLOAT            GENERATED ALWAYS AS (1::float / col21) STORED,
      col23       BOOLEAN          DEFAULT TRUE,
      PRIMARY KEY (col20)
    );

CREATE or REPLACE FUNCTION myTbl2trgfct1 () RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    INSERT INTO mySchema1.myTbl2b (col21) SELECT OLD.col21;
    RETURN OLD;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO mySchema1.myTbl2b (col21) SELECT NEW.col21;
    RETURN NEW;
  ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO mySchema1.myTbl2b (col21) SELECT NEW.col21;
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
CREATE TRIGGER myTbl2trg1
  AFTER INSERT OR UPDATE OR DELETE ON myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct1();

CREATE or REPLACE FUNCTION myTbl2trgfct2 () RETURNS trigger AS $$
BEGIN
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER myTbl2trg2
  BEFORE INSERT OR UPDATE ON myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct2();

ALTER TABLE mySchema1.myTbl2 DISABLE TRIGGER myTbl2trg2;

--
-- Second schema
--

DROP SCHEMA IF EXISTS mySchema2 CASCADE;
CREATE SCHEMA mySchema2;

SET search_path=mySchema2;

DROP TABLE IF EXISTS myTbl1 ;
CREATE TABLE myTbl1 (
  col11       DECIMAL (7)      NOT NULL,
  col12       CHAR (10)        NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (col11,col12)
);

DROP TABLE IF EXISTS myTbl2 ;
CREATE TABLE myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);

DROP TABLE IF EXISTS "myTbl3" ;
CREATE TABLE "myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32 desc, col33);

DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21) DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT mytbl4_col44_fkey FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL
);

DROP TABLE IF EXISTS myTbl5 ;
CREATE TABLE myTbl5 (
  col51       INT              NOT NULL,
  col52       TEXT[]           ,
  col53       INT[]            ,
  col54       DATE[]           ,
  col55       JSON             ,
  PRIMARY KEY (col51)
);

DROP TABLE IF EXISTS myTbl6 ;
CREATE TABLE myTbl6 (
  col61       INT4             NOT NULL,
  col62       POINT            ,
  col63       BOX              ,
  col64       CIRCLE           ,
  col65       PATH             ,
  col66       inet             ,
  PRIMARY KEY (col61)
);

-- This table will remain outside table groups and a foreign key will be created later in createDrop.sql
DROP TABLE IF EXISTS myTbl7 ;
CREATE TABLE myTbl7 (
  col71       INT              NOT NULL,
  PRIMARY KEY (col71)
);

-- This table will remain outside table groups and a foreign key will be created later in createDrop.sql
DROP TABLE IF EXISTS myTbl8 ;
CREATE TABLE myTbl8 (
  col81       INT              NOT NULL,
  PRIMARY KEY (col81)
);

CREATE SEQUENCE mySeq1 MINVALUE 1000 MAXVALUE 2000 CYCLE;

-- This sequence will remain outside any groups until the addition into a group in logging state
CREATE SEQUENCE mySeq2;

--
-- Third schema
--

DROP SCHEMA IF EXISTS "phil's schema3" CASCADE;
CREATE SCHEMA "phil's schema3";

SET search_path="phil's schema3";

DROP TABLE IF EXISTS "phil's tbl1" ;
CREATE TABLE "phil's tbl1" (
  "phil's col11" DECIMAL (7)      NOT NULL,
  "phil's col12" CHAR (10)        NOT NULL,
  "phil\s col13" BYTEA            ,
  PRIMARY KEY ("phil's col11","phil's col12")
);

DROP TABLE IF EXISTS "myTbl2\" ;
CREATE TABLE "myTbl2\" (
  col21       SERIAL           NOT NULL,
  col22       TEXT             ,
  col23       DATE
);

DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  CONSTRAINT mytbl4_col44_fkey FOREIGN KEY (col44,col45) REFERENCES "phil's tbl1" ("phil's col11","phil's col12") ON DELETE CASCADE ON UPDATE SET NULL
);
ALTER TABLE "myTbl2\" ADD CONSTRAINT mytbl2_col21_fkey FOREIGN KEY (col21) REFERENCES myTbl4 (col41);

CREATE SEQUENCE "phil's seq\1" MINVALUE 1000 MAXVALUE 2000 CYCLE;

--
-- Fourth schema (for partitioning)
--

DROP SCHEMA IF EXISTS mySchema4 CASCADE;
CREATE SCHEMA mySchema4;

SET search_path=mySchema4;

-- New partitionning style

DROP TABLE IF EXISTS myTblP;
CREATE TABLE myTblP (
  col1       INT              NOT NULL,
  col2       TEXT,
  col3       SERIAL
) PARTITION BY RANGE (col1);

DROP TABLE IF EXISTS myPartP1 ;
CREATE TABLE myPartP1 PARTITION OF myTblP (PRIMARY KEY (col1)) FOR VALUES FROM (MINVALUE) TO (0);

DROP TABLE IF EXISTS myPartP2 ;
CREATE TABLE myPartP2 PARTITION OF myTblP (PRIMARY KEY (col1)) FOR VALUES FROM (0) TO (9);
