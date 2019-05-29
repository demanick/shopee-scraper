SELECT
  p.id,
  p.name,
  pmap.catid,
  pmap.name,
  pmap.tier,
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
  shopee.products_2019_03_29 p
LEFT JOIN
  (
  SELECT
    pmap.productid,
    pmap.catid,
    c.name,
    c.tier
  FROM
    shopee.product_cat_map_2019_03_29 pmap
  LEFT JOIN
    shopee.categories c
  ON
    pmap.catid = c.id
  WHERE
    c.tier = 'main'
  ) pmap
ON
  p.id = pmap.productid
