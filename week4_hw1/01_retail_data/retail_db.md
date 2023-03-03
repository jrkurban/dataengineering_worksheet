# Sorular

## 1.
Spark dataframe ile `https://github.com/erkansirin78/datasets/tree/master/retail_db` veri setlerini kullanarak aşağıdaki iş ihtiyaçlarını çözünüz. `retail_db` içinde 6 adet csv dosyası bulunmaktadır. Siparişlerin iptal edildiği, ödeme beklediği gibi bilgiler `orders.csv` içindedir. Fatura detayları ise `order_items.csv` içindedir. 

### 1.1. `order_items` tablosunda kaç tane **tekil** `orderItemOrderId` vardır sayısını bulunuz.

### 1.2. `orders` ve `order_items` tablolarında kaç satır vardır bulunuz.

### 1.3. Toplam satış tutarı bakımından en çok iptal edilen (azalan sıra) ürünleri lokal diske `parquet` formatında yazınız.
```     
Örnek Sonuç:

|:-:|:---------------------------------------------:|:----------:|
| 0 | Field & Stream Sportsman 16 Gun Fire Safe     | 134393.28  |
| 1 | Perfect Fitness Perfect Rip Deck              | 85785.70   |
| 2 | Nike Men's Free 5.0+ Running Shoe             | 80691.93   |
| 3 | Diamondback Women's Serene Classic Comfort Bi | 80094.66   |
| 4 | Pelican Sunstream 100 Kayak                   | 66196.69   |
| 5 | Nike Men's Dri-FIT Victory Golf Polo          | 65750.00   |
| 6 | Nike Men's CJ Elite 2 TD Football Cleat       | 60705.33   |
| 7 | O'Brien Men's Neoprene Life Vest              | 58126.74   |
| 8 | Under Armour Girls' Toddler Spine Surge Runni | 26153.46   |
| 9 | LIJA Women's Eyelet Sleeveless Golf Polo      | 2145.00    |
```
### 1.4. Toplam satış tutarı bakımından en çok iptal edilen (azalan sıra) kategorileri local diske `parquet` formatında yazınız.
```
    Örnek sonuç:

|:-:|:----------------------------------------:|:----------:|
| 0 | Fishing                                  | 134393.28  |
| 1 | Cleats                                   | 85785.70   |
| 2 | Cardio Equipment                         | 81351.93   |
| 3 | Camping & Hiking                         | 80094.66   |
| 4 | Water Sports                             | 66196.69   |
| 5 | Women's Apparel                          | 65750.00   |
| 6 | Men's Footwear                           | 60705.33   |
| 7 | Indoor/Outdoor Games                     | 58126.74   |
| 8 | Shop By Sport                            | 27423.44   |
| 9 | Electronics                              | 5685.50    |
```

### 1.5. En yüksek toplam satış hangi yılın hangi ayında (Türkçe) olmuştur?

### 1.6. En yüksek toplam satış haftanın hangi gününde (Türkçe) olmuştur?


### 1.7. Bütün bu tablolardan mümkün olan en büyük tabloyu oluşturup hive test1 veri tabanına retail_all adında bir tabloya yazınız. 
Spark thrift ile DBeaver üzerinden üzerinde yukarıdaki iş ihtiyaçlarını sorgulayınız.
