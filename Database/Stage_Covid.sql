USE [master]
GO

DROP DATABASE IF EXISTS Stage_Covid
GO

CREATE DATABASE	Stage_Covid
GO

USE Stage_Covid
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[CaseDetailsCanadaStage]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].CaseDetailsCanadaStage
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[CaseReportStage]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].CaseReportStage
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[OutbreakStage]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].OutbreakStage
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHUStage]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHUStage
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHUGroupStage]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHUGroupStage
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[VaccinateStage]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].VaccinateStage
GO

CREATE TABLE dbo.CaseDetailsCanadaStage
	(
	CaseDetailsCanadaStageID bigint NOT NULL IDENTITY (1, 1),
	ObjectID bigint NULL,
	RowID bigint NULL,
	DateReported datetime NULL,
	HealthRegion varchar(128) NULL,
	AgeGroup varchar(50) NULL,
	Gender varchar(50) NULL,
	Exposure varchar(128) NULL,
	CaseStatus varchar(128) NULL,
	Province varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.CaseDetailsCanadaStage ADD CONSTRAINT
	PK_CaseDetailsCanadaStage PRIMARY KEY CLUSTERED 
	(
	CaseDetailsCanadaStageID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

CREATE TABLE dbo.CaseReportStage
	(
	CaseReportID bigint NOT NULL IDENTITY (1, 1),
	ReportingPHU varchar(128) NULL,
	PHUCity varchar(128) NULL,
	Outcome varchar(128) NULL,
	Age varchar(50) NULL,
	Gender varchar(50) NULL,
	CaseAcquisitionInfo varchar(128) NULL,
	SpecimenDate date NULL,
	CaseReportedDate date NULL,
	TestReportedDate date NULL,
	AccurateEpisodeDt date NULL,
	PHUAddress varchar(128) NULL,
	PHUWebsite varchar(128) NULL,
	PHULatitude varchar(128) NULL,
	PHULongitude varchar(128) NULL,
	PHUPostalCode varchar(128) NULL,
	OutbreakRelated varchar(12) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.CaseReportStage ADD CONSTRAINT
	PK_CaseReportStage PRIMARY KEY CLUSTERED 
	(
	CaseReportID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

CREATE TABLE dbo.OutbreakStage
	(
	OutbreakID bigint NOT NULL IDENTITY (1, 1),
	Date date NULL,
	PhuNum bigint NULL,
	OutbreakGroup varchar(128) NULL,
	NumberOngoingOutbreaks int NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.OutbreakStage ADD CONSTRAINT
	PK_OutbreakStage PRIMARY KEY CLUSTERED 
	(
	OutbreakID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

CREATE TABLE dbo.PHUStage
	(
	PHUIDKey bigint NOT NULL IDENTITY (1, 1),
	PHUID bigint NULL,
	ReportingPHU varchar(128) NULL,
	ReportingPHUAddress varchar(128) NULL,
	ReportingPHUCity varchar(128) NULL,
	ReportingPHUPostalCode varchar(128) NULL,
	ReportingPHUWebsite varchar(128) NULL,
	ReportingPHULatitude varchar(128) NULL,
	ReportingPHULongitude varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.PHUStage ADD CONSTRAINT
	PK_PHUStage PRIMARY KEY CLUSTERED 
	(
	PHUIDKey
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

CREATE TABLE dbo.PHUGroupStage
	(
	PHUGroupID bigint NOT NULL IDENTITY (1, 1),
	PHUGroup varchar(128) NULL,
	PHUCity varchar(128) NULL,
	PHURegion varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.PHUGroupStage ADD CONSTRAINT
	PK_PHUGroupStage PRIMARY KEY CLUSTERED 
	(
	PHUGroupID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

CREATE TABLE dbo.VaccinateStage
	(
	VaccinateID bigint NOT NULL IDENTITY (1, 1),
	Date date NULL,
	PHUID bigint NULL,
	AgeGroup varchar(50) NULL,
	AtLeastOneDoseCumulative int NULL,
	SecondDoseCumulative int NULL,
	FullyVaccinatedCumulative int NULL,
	ThirdDoseCumulative int NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.VaccinateStage ADD CONSTRAINT
	PK_VaccinateStage PRIMARY KEY CLUSTERED 
	(
	VaccinateID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO