USE NDS_Covid
GO

ALTER TABLE PHU
ADD PHUProvince varchar(128)
GO

CREATE TABLE #PHUProvince (PHUName varchar(128),Province varchar(128))

INSERT INTO #PHUProvince VALUES
('Simcoe Muskoka District Health Unit','Ontario'),
('Hamilton Public Health Services', 'Ontario'),
('Leeds, Grenville and Lanark District Health Unit','Ontario'),
('York Region Public Health Services','Ontario'),
('Lambton Public Health','Ontario'),
('Peterborough Public Health','Ontario'),
('Eastern Ontario Health Unit','Ontario'),
('Middlesex-London Health Unit','Ontario'),
('Timiskaming Health Unit','Ontario'),
('Interior','British Columbia'),
('Terres-Cries-de-la-Baie-James','Quebec'),
('Bas-Saint-Laurent','Quebec'),
('Estrie','Quebec'),
('Abitibi-Témiscamingue','Quebec'),
('Nunavik','Quebec'),
('Montérégie','Quebec'),
('Chatham-Kent Health Unit','Ontario'),
('Wellington-Dufferin-Guelph Public Health','Ontario'),
('Saguenay','Quebec'),
('Calgary Zone','Alberta'),
('Nord-du-Québec','Quebec'),
('Vancouver Island','British Columbia'),
('Chaudière-Appalaches','Quebec'),
('Northern','British Columbia'),
('Lanaudière','Quebec'),
('Out of Canada','British Columbia'),
('Region of Waterloo, Public Health','Ontario'),
('Haliburton, Kawartha, Pine Ridge District Health Unit','Ontario'),
('Sudbury & District Health Unit','Ontario'),
('Kingston, Frontenac and Lennox & Addington Public Health','Ontario'),
('Toronto Public Health','Ontario'),
('Peel Public Health','Ontario'),
('Haldimand-Norfolk Health Unit','Ontario'),
('Renfrew County and District Health Unit','Ontario'),
('Windsor-Essex County Health Unit','Ontario'),
('Durham Region Health Department','Ontario'),
('Northwestern Health Unit','Ontario'),
('Thunder Bay District Health Unit','Ontario'),
('Brant County Health Unit','Ontario'),
('Huron Perth District Health Unit','Ontario'),
('Hastings and Prince Edward Counties Health Unit','Ontario'),
('Halton Region Health Department','Ontario'),
('Southwestern Public Health','Ontario'),
('Niagara Region Public Health Department','Ontario'),
('Algoma Public Health Unit','Ontario'),
('Ottawa Public Health','Ontario'),
('Grey Bruce Health Unit','Ontario'),
('North Bay Parry Sound District Health Unit','Ontario'),
('Porcupine Health Unit','Ontario'),
('Laurentides','Quebec'),
('Côte-Nord','Quebec'),
('South Zone',	'Alberta'),
('Gaspésie-Îles-de-la-Madeleine','Quebec'),
('Capitale-Nationale','Quebec'),
('Montréal','Quebec'),
('Laval','Quebec'),
('Fraser','British Columbia'),
('Central Zone','Alberta'),
('Not Reported','Quebec'),
('North Zone','Alberta'),
('Unknown','Alberta'),
('Outaouais','Quebec'),
('Edmonton Zone','Alberta'),
('Mauricie','Quebec'),
('Vancouver Coastal','British Columbia')

UPDATE PHU with(rowlock)
SET PHU.PHUProvince = tbl.Province
FROM PHU 
JOIN #PHUProvince tbl ON PHU.PHUName = tbl.PHUName

SELECT * FROM PHU

DROP TABLE #PHUProvince
GO

USE DDS_Covid
GO

ALTER TABLE PHU
ADD PHUProvince varchar(128)
GO

CREATE TABLE #PHUProvince (PHUName varchar(128),Province varchar(128))

INSERT INTO #PHUProvince VALUES
('Simcoe Muskoka District Health Unit','Ontario'),
('Hamilton Public Health Services', 'Ontario'),
('Leeds, Grenville and Lanark District Health Unit','Ontario'),
('York Region Public Health Services','Ontario'),
('Lambton Public Health','Ontario'),
('Peterborough Public Health','Ontario'),
('Eastern Ontario Health Unit','Ontario'),
('Middlesex-London Health Unit','Ontario'),
('Timiskaming Health Unit','Ontario'),
('Interior','British Columbia'),
('Terres-Cries-de-la-Baie-James','Quebec'),
('Bas-Saint-Laurent','Quebec'),
('Estrie','Quebec'),
('Abitibi-Témiscamingue','Quebec'),
('Nunavik','Quebec'),
('Montérégie','Quebec'),
('Chatham-Kent Health Unit','Ontario'),
('Wellington-Dufferin-Guelph Public Health','Ontario'),
('Saguenay','Quebec'),
('Calgary Zone','Alberta'),
('Nord-du-Québec','Quebec'),
('Vancouver Island','British Columbia'),
('Chaudière-Appalaches','Quebec'),
('Northern','British Columbia'),
('Lanaudière','Quebec'),
('Out of Canada','British Columbia'),
('Region of Waterloo, Public Health','Ontario'),
('Haliburton, Kawartha, Pine Ridge District Health Unit','Ontario'),
('Sudbury & District Health Unit','Ontario'),
('Kingston, Frontenac and Lennox & Addington Public Health','Ontario'),
('Toronto Public Health','Ontario'),
('Peel Public Health','Ontario'),
('Haldimand-Norfolk Health Unit','Ontario'),
('Renfrew County and District Health Unit','Ontario'),
('Windsor-Essex County Health Unit','Ontario'),
('Durham Region Health Department','Ontario'),
('Northwestern Health Unit','Ontario'),
('Thunder Bay District Health Unit','Ontario'),
('Brant County Health Unit','Ontario'),
('Huron Perth District Health Unit','Ontario'),
('Hastings and Prince Edward Counties Health Unit','Ontario'),
('Halton Region Health Department','Ontario'),
('Southwestern Public Health','Ontario'),
('Niagara Region Public Health Department','Ontario'),
('Algoma Public Health Unit','Ontario'),
('Ottawa Public Health','Ontario'),
('Grey Bruce Health Unit','Ontario'),
('North Bay Parry Sound District Health Unit','Ontario'),
('Porcupine Health Unit','Ontario'),
('Laurentides','Quebec'),
('Côte-Nord','Quebec'),
('South Zone',	'Alberta'),
('Gaspésie-Îles-de-la-Madeleine','Quebec'),
('Capitale-Nationale','Quebec'),
('Montréal','Quebec'),
('Laval','Quebec'),
('Fraser','British Columbia'),
('Central Zone','Alberta'),
('Not Reported','Quebec'),
('North Zone','Alberta'),
('Unknown','Alberta'),
('Outaouais','Quebec'),
('Edmonton Zone','Alberta'),
('Mauricie','Quebec'),
('Vancouver Coastal','British Columbia')

UPDATE PHU with(rowlock)
SET PHU.PHUProvince = tbl.Province
FROM PHU 
JOIN #PHUProvince tbl ON PHU.PHUName = tbl.PHUName

SELECT * FROM PHU

DROP TABLE #PHUProvince
GO