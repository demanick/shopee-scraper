SELECT
  p.id,
  p.name,
  pmap.catid,
  pmap.catname,
  pmap.catlevel,
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
  (
  SELECT
    pmap.productid,
    pmap.catid,
    c.catname,
    c.catlevel
  FROM
    shopee.productid_map_2019_02_21 pmap
  LEFT JOIN
    shopee.categories_2019_02_21 c
  ON
    pmap.catid = c.catid
  WHERE
    c.catlevel= 'main'
  ) pmap
ON
  p.id = pmap.productid
