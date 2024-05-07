USE [master]
GO

DROP DATABASE IF EXISTS METADATA_Covid
GO

CREATE DATABASE	METADATA_Covid
GO

USE METADATA_Covid
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[DDSCovid]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].DDSCovid
GO

CREATE TABLE dbo.DDSCovid
	(
	DDSCovidID bigint NOT NULL IDENTITY (1, 1),
	TableName varchar(126),
	CreatedDate datetime NULL,
	ModifiedDate datetime NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.DDSCovid ADD CONSTRAINT
	PK_DDSCovidCaseDetailsCanadaStage PRIMARY KEY CLUSTERED 
	(
	DDSCovidID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

INSERT INTO DDSCovid VALUES 
('AgeGroup',1,1),
('City',1,1),
('Date',1,1),
('Gender',1,1),
('OutbreakGroup',1,1),
('PHU',1,1),
('PHUGroup',1,1),
('StatisticCaseAcquisition',1,1),
('StatisticOutbreak',1,1),
('StatisticVaccinate',1,1)

GO