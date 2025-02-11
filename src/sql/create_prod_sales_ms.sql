CREATE OR REPLACE TABLE {PRD_TABLE}  AS
  SELECT
          CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP,
          Provider AS PROVIDER  ,
          "Provider Country" AS PROVIDER_COUNTRY,
          SKU AS SKU  ,
          Developer AS DEVELOPER  ,
          Title AS TITLE  ,
          Version AS VERSION  ,
          "Product Type Identifier" AS PRODUCT_TYPE_IDENTIFIER,
          Units AS UNITS  ,
          "Developer Proceeds" AS DEVELOPER_PROCEEDS,
          TO_DATE("Begin Date") AS BEGIN_DATE,
          TO_DATE("End Date") AS END_DATE,
          "Customer Currency" AS CUSTOMER_CURRENCY,
          "Country Code" AS COUNTRY_CODE,
          "Currency of Proceeds" AS CURRENCY_OF_PROCEEDS,
          "Apple Identifier" AS APPLE_IDENTIFIER,
          "Customer Price" AS CUSTOMER_PRICE,
          "Promo Code" AS PROMO_CODE,
          "Parent Identifier" AS PARENT_IDENTIFIER,
          Subscription AS SUBSCRIPTION  ,
          Period AS PERIOD  ,
          Category AS CATEGORY  ,
          CMB AS CMB  ,
          Device AS DEVICE  ,
          "Supported Platforms" AS SUPPORTED_PLATFORMS,
          "Proceeds Reason" AS PROCEEDS_REASON,
          "Preserved Pricing" AS PRESERVED_PRICING,
          Client AS CLIENT,
          "Order Type" AS ORDER_TYPE
        FROM
            {TEMP_TABLE}
