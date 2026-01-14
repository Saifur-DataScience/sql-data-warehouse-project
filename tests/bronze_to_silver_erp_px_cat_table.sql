-- Finally, we clean the last ERP table and 
-- insert values in the Silver layer

SELECT * FROM Bronze.erp_px_cat_g1v2; 


-- Check for data inconsistencies in the columns

SELECT *
FROM Bronze.erp_px_cat_g1v2
WHERE id != TRIM(id) OR id IS NULL OR LEN(id) != 5; 

SELECT *
FROM Bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR cat IS NULL; 

SELECT DISTINCT cat
FROM Bronze.erp_px_cat_g1v2; 

SELECT *
FROM Bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat) OR subcat IS NULL; 

SELECT DISTINCT subcat
FROM Bronze.erp_px_cat_g1v2; 

SELECT *
FROM Bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance) OR maintenance IS NULL; 

SELECT DISTINCT maintenance
FROM Bronze.erp_px_cat_g1v2; 


-- All the columns are already clean and need no transformations
-- Insert the data in the Silver layer table

INSERT INTO Silver.erp_px_cat_g1v2(
	id, 
	cat, 
	subcat, 
	maintenance
)
SELECT * FROM Bronze.erp_px_cat_g1v2; 

SELECT * FROM Silver.erp_px_cat_g1v2; 
