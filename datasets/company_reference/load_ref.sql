-- Truncate existing data
TRUNCATE TABLE ref.company_main;

-- Insert company data
INSERT INTO ref.company_main (company_id, company_name, parent_company_id, address) VALUES
('PERC', 'PetroEnergy Resources Corp', NULL, NULL),
('PGEC', 'PetroGreen Energy Corp', 'PERC', NULL),
('PSC', 'PetroSolar Corp', 'PGEC', NULL),
('PWEI', 'PetroWind Energy Inc.', 'PGEC', NULL),
('MGI', 'Maibarara Geothermal Inc.', 'PGEC', NULL),
('ESEC', 'EcoSolar Energy Corp', 'PGEC', NULL),
('RGEC', 'Rizal Green Energy Corp', 'PGEC', NULL),
('BEP_NL', 'Buhawind Energy Phillippines (Northern Luzon)', 'PGEC', NULL),
('BEP_NM', 'Buhawind Energy Phillippines (Northern Mindoro)', 'PGEC', NULL),
('BEP_EP', 'Buhawind Energy Phillippines (East Panay)', 'PGEC', NULL);