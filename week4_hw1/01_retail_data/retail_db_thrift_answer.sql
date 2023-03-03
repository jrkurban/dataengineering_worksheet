SELECT  * FROM  test1.retail_all limit 5;


-- Toplam satış tutarı bakımından en çok iptal edilen (azalan sıra) ürünler
SELECT productName, SUM(orderItemSubTotal) orderItemSubTotal FROM  test1.retail_all
WHERE orderStatus == 'CANCELED'
GROUP BY productName
ORDER BY orderItemSubTotal DESC 
LIMIT 10;
/*
productName                                  |orderItemSubTotal |
---------------------------------------------|------------------|
Field & Stream Sportsman 16 Gun Fire Safe    |134393.27999999965|
Perfect Fitness Perfect Rip Deck             | 85785.70000000016|
Nike Men's Free 5.0+ Running Shoe            | 80691.93000000001|
Diamondback Women's Serene Classic Comfort Bi|  80094.6600000001|
Pelican Sunstream 100 Kayak                  |  66196.6899999998|
Nike Men's Dri-FIT Victory Golf Polo         |           65750.0|
Nike Men's CJ Elite 2 TD Football Cleat      | 60705.32999999974|
O'Brien Men's Neoprene Life Vest             | 58126.74000000006|
Under Armour Girls' Toddler Spine Surge Runni| 26153.45999999998|
LIJA Women's Eyelet Sleeveless Golf Polo     |            2145.0|
*/


-- Toplam satış tutarı bakımından en çok iptal edilen (azalan sıra) kategoriler
SELECT categoryName, SUM(orderItemSubTotal) orderItemSubTotal FROM  test1.retail_all
WHERE orderStatus == 'CANCELED'
GROUP BY categoryName
ORDER BY orderItemSubTotal DESC 
LIMIT 10;
/*
categoryName        |orderItemSubTotal |
--------------------|------------------|
Fishing             |134393.27999999965|
Cleats              | 85785.70000000016|
Cardio Equipment    | 81351.93000000001|
Camping & Hiking    |  80094.6600000001|
Water Sports        |  66196.6899999998|
Women's Apparel     |           65750.0|
Men's Footwear      | 60705.32999999974|
Indoor/Outdoor Games| 58126.74000000006|
Shop By Sport       | 27423.43999999998|
Electronics         | 5685.499999999998|
 */

-- En yüksek toplam satış hangi yılın hangi ayında (Türkçe) olmuştur?

SELECT CASE OrderMonth
WHEN 1 THEN "Ocak"
WHEN 2 THEN "Şubat"
WHEN 3 THEN "Mart"
WHEN 4 THEN "Nisan"
WHEN 5 THEN "Mayıs"
WHEN 6 THEN "Haziran"
WHEN 7 THEN "Temmuz"
WHEN 8 THEN "Ağustos"
WHEN 9 THEN "Eylül"
WHEN 10 THEN "Ekim"
WHEN 11 THEN "Kasım"
WHEN 12 THEN "Aralık"
END As TR_MONTH, Sum_SubTotal 
FROM (
SELECT MONTH(orderDate) as OrderMonth, SUM(orderItemSubTotal) Sum_SubTotal FROM test1.retail_all
WHERE orderStatus != 'CANCELED'
GROUP BY OrderMonth 
ORDER BY Sum_SubTotal DESC) a
LIMIT 10;
/*
TR_MONTH|Sum_SubTotal      |
--------|------------------|
Kasım   |3105843.2700001677|
Temmuz  |2941078.6900001527|
Ocak    |2870834.1800001645|
Aralık  | 2869997.880000174|
Eylül   | 2866553.330000173|
Mart    |2805006.3200001693|
Ağustos | 2769236.030000181|
Nisan   | 2758912.470000167|
Şubat   |2712838.5800001603|
Mayıs   |2695699.4800001844|
 */


-- En yüksek toplam satış haftanın hangi gününde (Türkçe) olmuştur?

SELECT CASE WeekDay
WHEN 1 THEN "Pazartesi"
WHEN 2 THEN "Salı"
WHEN 3 THEN "Çarşamba"
WHEN 4 THEN "Perşembe"
WHEN 5 THEN "Cuma"
WHEN 6 THEN "Cumartesi"
WHEN 7 THEN "Pazar"
END As TR_DAY, Sum_SubTotal FROM (
SELECT dayofweek(orderDate) as WeekDay, SUM(orderItemSubTotal) Sum_SubTotal FROM test1.retail_all
WHERE orderStatus != 'CANCELED'
GROUP BY WeekDay 
ORDER BY Sum_SubTotal DESC) a
LIMIT 10;
/*
 * TR_DAY   |Sum_SubTotal     |
---------|-----------------|
Cumartesi|5065099.070000702|
Cuma     |4878164.550000709|
Pazar    |4862228.110000682|
Çarşamba |4809499.660000661|
Perşembe |4805157.070000659|
Pazartesi| 4750554.52000066|
Salı     |4455885.960000647|
*/











