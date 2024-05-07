USE [master]
GO

DROP DATABASE IF EXISTS DDS_Covid 

CREATE DATABASE DDS_Covid
GO

USE DDS_Covid
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[AgeGroup]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].AgeGroup
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[City]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].City
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[Date]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].Date
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[Gender]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].Gender
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHU]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHU
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHUGroup]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHUGroup
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[StatisticCaseAcquisition]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].StatisticCaseAcquisition
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[StatisticOutbreak]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].StatisticOutbreak
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[OutbreakGroup]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].OutbreakGroup
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[StatisticOutbreak]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].StatisticSeriousLevel
GO

CREATE TABLE dbo.AgeGroup
	(
	AgeGroupID bigint NOT NULL IDENTITY (1, 1),
	FromAge varchar(32) NULL,
	ToAge varchar(32) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.AgeGroup ADD CONSTRAINT
	PK_AgeGroup PRIMARY KEY CLUSTERED 
	(
	AgeGroupID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
CREATE TABLE dbo.City
	(
	CityID bigint NOT NULL IDENTITY (1, 1),
	PHUGroupID bigint NULL,
	CityName varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.City ADD CONSTRAINT
	PK_City PRIMARY KEY CLUSTERED 
	(
	CityID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
CREATE TABLE dbo.Date
	(
	DateID bigint NOT NULL IDENTITY (1, 1),
	DayOfMonth int NULL,
	MonthOfYear int NULL,
	QuarterOfYear int NULL,
	Year int NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.Date ADD CONSTRAINT
	PK_Date PRIMARY KEY CLUSTERED 
	(
	DateID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
CREATE TABLE dbo.Gender
	(
	GenderID bigint NOT NULL IDENTITY (1, 1),
	GenderName varchar(50) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.Gender ADD CONSTRAINT
	PK_Gender PRIMARY KEY CLUSTERED 
	(
	GenderID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO

CREATE TABLE dbo.PHU
	(
	PHUID bigint NOT NULL IDENTITY (1, 1),
	CityID bigint NULL,
	PHUName varchar(128) NULL,
	PHUAddress varchar(128) NULL,
	PHUPostalCode varchar(128) NULL,
	PHUWebsite varchar(128) NULL,
	PHULatitude varchar(128) NULL,
	PHULongitude varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.PHU ADD CONSTRAINT
	PK_PHU PRIMARY KEY CLUSTERED 
	(
	PHUID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
CREATE TABLE dbo.PHUGroup
	(
	PHUGroupID bigint NOT NULL IDENTITY (1, 1),
	PHUGroupName varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.PHUGroup ADD CONSTRAINT
	PK_PHUGroup PRIMARY KEY CLUSTERED 
	(
	PHUGroupID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
CREATE TABLE dbo.OutbreakGroup
	(
	OutbreakGroupID bigint NOT NULL IDENTITY (1, 1),
	OutbreakGroupName varchar(128) NULL
	)  ON [PRIMARY]
GO
ALTER TABLE dbo.OutbreakGroup ADD CONSTRAINT
	PK_OutbreakGroup PRIMARY KEY CLUSTERED 
	(
	OutbreakGroupID
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
CREATE TABLE dbo.StatisticCaseAcquisition
	(
	PHUID bigint NOT NULL,
	DateID bigint NOT NULL,
	AgeGroupID bigint NOT NULL,
	GenderID bigint NOT NULL,
	TotalDeath bigint NULL,
	TotalRecovery bigint NULL,
	TotalInfection bigint NULL,
	TotalCase bigint NULL
	)  ON [PRIMARY]
GO
CREATE TABLE dbo.StatisticOutbreak
	(
	PHUID bigint NOT NULL,
	DateID bigint NOT NULL,
	OutbreakGroupID bigint NOT NULL,
	TotalOnGoingOutbreaks bigint NULL
	)  ON [PRIMARY]
GO
CREATE TABLE dbo.StatisticVaccinate
	(
	PHUID bigint NOT NULL,
	DateID bigint NOT NULL,
	AgeGroupID bigint NOT NULL,
	AtLeastOneVaccinated decimal(34, 2) NULL,
	SecondDoseCumulative decimal(34, 2) NULL,
	ThirdDoseCumulative decimal(34, 2) NULL,
	FullyVaccinatedCumulative decimal(34, 2) NULL
	)  ON [PRIMARY]
GO