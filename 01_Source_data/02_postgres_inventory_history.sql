-- =========================================
-- CREATE TABLE: inventory
-- =========================================

DROP TABLE IF EXISTS inventory;

CREATE TABLE inventory (
    inventory_id           VARCHAR(20) PRIMARY KEY,
    product_id             VARCHAR(20),
    store_id               VARCHAR(20),
    stock_quantity         INTEGER,
    reorder_level          INTEGER,
    warehouse_location     VARCHAR(100),
    last_stock_update      TIMESTAMP
);

-- =========================================
-- INSERT DATA
-- =========================================

INSERT INTO inventory (
    inventory_id,
    product_id,
    store_id,
    stock_quantity,
    reorder_level,
    warehouse_location,
    last_stock_update
)
VALUES
('INV0001','P0001','S001',45,10,'North Warehouse','2026-01-01 10:00:00'),
('INV0002','P0002','S001',30,8,'North Warehouse','2026-01-01 10:00:00'),
('INV0003','P0003','S001',55,15,'North Warehouse','2026-01-01 10:00:00'),
('INV0004','P0004','S001',12,5,'North Warehouse','2026-01-01 10:00:00'),
('INV0005','P0005','S001',18,5,'North Warehouse','2026-01-01 10:00:00'),

('INV0006','P0006','S002',60,20,'West Warehouse','2026-01-02 11:00:00'),
('INV0007','P0007','S002',75,25,'West Warehouse','2026-01-02 11:00:00'),
('INV0008','P0008','S002',40,10,'West Warehouse','2026-01-02 11:00:00'),
('INV0009','P0009','S002',150,50,'West Warehouse','2026-01-02 11:00:00'),
('INV0010','P0010','S002',120,40,'West Warehouse','2026-01-02 11:00:00'),

('INV0011','P0011','S003',25,8,'South Warehouse','2026-01-03 12:00:00'),
('INV0012','P0012','S003',10,3,'South Warehouse','2026-01-03 12:00:00'),
('INV0013','P0013','S003',22,7,'South Warehouse','2026-01-03 12:00:00'),
('INV0014','P0014','S003',16,5,'South Warehouse','2026-01-03 12:00:00'),
('INV0015','P0015','S003',35,10,'South Warehouse','2026-01-03 12:00:00'),

('INV0016','P0016','S004',48,15,'East Warehouse','2026-01-04 09:30:00'),
('INV0017','P0017','S004',90,30,'East Warehouse','2026-01-04 09:30:00'),
('INV0018','P0018','S004',65,20,'East Warehouse','2026-01-04 09:30:00'),
('INV0019','P0019','S004',200,60,'East Warehouse','2026-01-04 09:30:00'),
('INV0020','P0020','S004',28,8,'East Warehouse','2026-01-04 09:30:00'),

('INV0021','P0001','S005',38,10,'Central Warehouse','2026-01-05 14:00:00'),
('INV0022','P0002','S005',26,8,'Central Warehouse','2026-01-05 14:00:00'),
('INV0023','P0003','S005',42,12,'Central Warehouse','2026-01-05 14:00:00'),
('INV0024','P0004','S005',14,5,'Central Warehouse','2026-01-05 14:00:00'),
('INV0025','P0005','S005',20,5,'Central Warehouse','2026-01-05 14:00:00'),

('INV0026','P0006','S001',58,20,'North Warehouse','2026-01-06 10:15:00'),
('INV0027','P0007','S001',72,25,'North Warehouse','2026-01-06 10:15:00'),
('INV0028','P0008','S001',36,10,'North Warehouse','2026-01-06 10:15:00'),
('INV0029','P0009','S001',170,50,'North Warehouse','2026-01-06 10:15:00'),
('INV0030','P0010','S001',110,40,'North Warehouse','2026-01-06 10:15:00'),

('INV0031','P0011','S002',20,8,'West Warehouse','2026-01-07 13:00:00'),
('INV0032','P0012','S002',12,3,'West Warehouse','2026-01-07 13:00:00'),
('INV0033','P0013','S002',18,7,'West Warehouse','2026-01-07 13:00:00'),
('INV0034','P0014','S002',15,5,'West Warehouse','2026-01-07 13:00:00'),
('INV0035','P0015','S002',30,10,'West Warehouse','2026-01-07 13:00:00'),

('INV0036','P0016','S003',44,15,'South Warehouse','2026-01-08 15:00:00'),
('INV0037','P0017','S003',85,30,'South Warehouse','2026-01-08 15:00:00'),
('INV0038','P0018','S003',62,20,'South Warehouse','2026-01-08 15:00:00'),
('INV0039','P0019','S003',210,60,'South Warehouse','2026-01-08 15:00:00'),
('INV0040','P0020','S003',24,8,'South Warehouse','2026-01-08 15:00:00');


-- Invalid records for DQ testing
INSERT INTO inventory VALUES
(NULL, 101, 'STORE_01', 10, 5, 'Warehouse-A', CURRENT_TIMESTAMP),
(9991, NULL, 'STORE_02', 20, 5, 'Warehouse-B', CURRENT_TIMESTAMP),
(9992, 102, NULL, 15, 5, 'Warehouse-C', CURRENT_TIMESTAMP),
(9993, 103, 'STORE_03', -5, 10, 'Warehouse-D', CURRENT_TIMESTAMP);
