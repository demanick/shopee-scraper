SELECT
  p.id,
  p.name,
  c.catid,
  c.catname,
  c.catlevel,
  p.shopid,
  p.price_min,
  p.price_max,
  p.discount,
  p.currency,
  p.rating,
  p.rating_count,
  p.comments,
  p.likes,
  p.sold,
  p.stock,
  p.free_shipping
FROM
  shopee.products_2019_02_08 p
LEFT JOIN
  shopee.productid_map_2019_02_21 pmap
ON
  p.id = pmap.productid
LEFT JOIN
  shopee.categories_2019_02_21 c
ON
  c.id = pmap.catid
WHERE
  c.catlevel = 'main' OR c.catlevel IS NULL
