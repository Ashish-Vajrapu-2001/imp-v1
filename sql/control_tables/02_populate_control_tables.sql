/*
  FILE: sql/control_tables/02_populate_control_tables.sql
  PURPOSE: Populate metadata from CLV Analytics Document
*/

-- Source Systems
INSERT INTO control.source_systems (source_system_name, source_system_type) VALUES
('ERP', 'Azure SQL Database'),
('CRM', 'Azure SQL Database'),
('MARKETING', 'Azure SQL Database');
GO

DECLARE @ERP_ID INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'ERP');
DECLARE @CRM_ID INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'CRM');
DECLARE @MKT_ID INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'MARKETING');

-- Table Metadata - CRM Tables
INSERT INTO control.table_metadata (source_system_id, schema_name, table_name, primary_key_columns, initial_load_completed) VALUES
(@CRM_ID, 'CRM', 'Customers', 'CUSTOMER_ID', 0),
(@CRM_ID, 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID', 0),
(@CRM_ID, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 0),
(@CRM_ID, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 0),
(@CRM_ID, 'CRM', 'SURVEYS', 'SURVEY_ID', 0);

-- Table Metadata - ERP Tables
INSERT INTO control.table_metadata (source_system_id, schema_name, table_name, primary_key_columns, initial_load_completed) VALUES
(@ERP_ID, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 0),
(@ERP_ID, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 0),
(@ERP_ID, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 0),
(@ERP_ID, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 0),
(@ERP_ID, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 0),
(@ERP_ID, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 0),
(@ERP_ID, 'ERP', 'BRANDS', 'BRAND_ID', 0);

-- Table Metadata - Marketing Tables
INSERT INTO control.table_metadata (source_system_id, schema_name, table_name, primary_key_columns, initial_load_completed) VALUES
(@MKT_ID, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 0);
GO

-- Load Dependencies
DECLARE @T_Cust INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'Customers');
DECLARE @T_Reg INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CustomerRegistrationSource');
DECLARE @T_Inc INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'INCIDENTS');
DECLARE @T_Int INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'INTERACTIONS');
DECLARE @T_Surv INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'SURVEYS');
DECLARE @T_OrdH INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_HEADERS_ALL');
DECLARE @T_OrdL INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_LINES_ALL');
DECLARE @T_Addr INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'ADDRESSES');
DECLARE @T_City INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CITY_TIER_MASTER');
DECLARE @T_Item INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'MTL_SYSTEM_ITEMS_B');
DECLARE @T_Cat INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CATEGORIES');
DECLARE @T_Brand INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'BRANDS');
DECLARE @T_Camp INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'MARKETING_CAMPAIGNS');

-- Insert Dependencies
INSERT INTO control.load_dependencies (table_id, depends_on_table_id) VALUES
(@T_Reg, @T_Cust),
(@T_Reg, @T_Camp),
(@T_Inc, @T_Cust),
(@T_Int, @T_Inc),
(@T_Surv, @T_Cust),
(@T_OrdH, @T_Cust),
(@T_OrdH, @T_Addr),
(@T_OrdL, @T_OrdH),
(@T_OrdL, @T_Item),
(@T_Addr, @T_City),
(@T_Item, @T_Cat),
(@T_Item, @T_Brand);
GO
