USE [master]
GO
/****** Object:  Database [NDS_Covid]    Script Date: 27/11/2022 11:43:39 SA ******/
DROP DATABASE IF EXISTS NDS_Covid
GO

CREATE DATABASE [NDS_Covid]
GO
USE [NDS_Covid]
GO
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[AgeGroup]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].AgeGroup
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[CaseAccquisition]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].CaseAccquisition
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[CaseReport]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].CaseReport
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[OutbreakGroup]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].OutbreakGroup
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHU]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHU
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHUGroup]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHUGroup
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[PHUCity]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].PHUCity
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[Vaccinate]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].Vaccinate
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[OutbreakPHU]') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)
  DROP TABLE [dbo].OutbreakPHU
GO

/****** Object:  Table [dbo].[AgeGroup]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[AgeGroup](
	[AgeGroupID] [bigint] NOT NULL IDENTITY (1, 1),
	[ToAge] [bigint] NULL,
	[FromAge] [bigint] NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_AgeGroup] PRIMARY KEY CLUSTERED 
(
	[AgeGroupID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CaseAccquisition]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CaseAcquisition](
	[CaseAcquisitionID] [bigint] NOT NULL IDENTITY (1, 1),
	[CaseAcquisitionInfo] [varchar](128) NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_CaseAccquisition] PRIMARY KEY CLUSTERED 
(
	[CaseAcquisitionID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CaseReport]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CaseReport](
	[CaseReportID] [bigint] NOT NULL IDENTITY (1, 1),
	[AgeGroupID] [bigint] NOT NULL,
	[CaseAcquisitionID] [bigint] NOT NULL,
	[PHUID] [bigint] NOT NULL,
	[Gender] [varchar](50) NULL,
	[SpecimenDate] [date] NULL,
	[CaseReportedDate] [date] NULL,
	[TestReportedDate] [date] NULL,
	[AccurateEpisodeDt] [date] NULL,
	[OutbreakRelated] [varchar](12) NULL,
	[CaseStatus] [varchar](128) NULL,
	[Province] [varchar](128) NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_CaseReport] PRIMARY KEY CLUSTERED 
(
	[CaseReportID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[OutbreakGroup]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[OutbreakGroup](
	[OutbreakGroupID] [bigint] NOT NULL IDENTITY (1, 1),
	[OutbreakGroupName] [varchar](128) NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_OutbreakGroup] PRIMARY KEY CLUSTERED 
(
	[OutbreakGroupID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[OutbreakPHU]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[OutbreakPHU](
	[OutbreakGroupID] [bigint] NOT NULL,
	[PHUID] [bigint] NOT NULL,
	[OutbreakDate] [date] NOT NULL,
	[NumberOnGoingOutbreaks] [int] NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,

 CONSTRAINT [PK_OutbreakPHU] PRIMARY KEY CLUSTERED 
(
	[PHUID] ASC,
	[OutbreakGroupID] ASC,
	[OutbreakDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PHU]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PHU](
	[PHUID] [bigint] NOT NULL IDENTITY (1, 1),
	[PHUCityID] [bigint] NULL,
	[PHUName] [varchar](128) NULL,
	[PHUAddress] [varchar](128) NULL,
	[PHUPostalCode] [varchar](128) NULL,
	[PHUWebsite] [varchar](128) NULL,
	[PHULatitude] [varchar](128) NULL,
	[PHULongitude] [varchar](128) NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_PHU] PRIMARY KEY CLUSTERED 
(
	[PHUID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PHUCity]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PHUCity](
	[PHUCityID] [bigint] NOT NULL IDENTITY (1, 1),
	[PHUGroupID] [bigint] NULL,
	[PHUCityName] [varchar](128) NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_PHUCity] PRIMARY KEY CLUSTERED 
(
	[PHUCityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PHUGroup]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PHUGroup](
	[PHUGroupID] [bigint] NOT NULL IDENTITY (1, 1),
	[PHUGroupName] [varchar](128) NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_PHUGroup] PRIMARY KEY CLUSTERED 
(
	[PHUGroupID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Vaccinate]    Script Date: 27/11/2022 11:43:40 SA ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Vaccinate](
	[PHUID] [bigint] NOT NULL,
	[AgeGroupID] [bigint] NOT NULL,
	[Date] [date] NOT NULL,
	[AtLeastOneVaccinated] [int] NULL,
	[SecondDoseCumulative] [int] NULL,
	[ThirdDoseCumulative] [int] NULL,
	[FullyVaccinatedCumulative] [int] NULL,
	[CreatedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_Vaccinate] PRIMARY KEY CLUSTERED 
(
	[PHUID] ASC,
	[AgeGroupID] ASC,
	[Date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO