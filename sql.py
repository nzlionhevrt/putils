from dateTime import datetime, timedelta

DATE_FRMT = '%Y-%m-%d'

def query_leads_from_calls(days=1, date_format=DATE_FRMT):
    """
    Query to select unique leads from calls.
    """
    dateFrom = (datetime.today()-timedelta(days)).strftime(date_format)
    dateTo = datetime.today().strftime(date_format)

    return """SELECT EXTRACT(EPOCH FROM min("StartTime")) as date,
           "CallingPhoneNumber" as phone
    FROM "Calls" AS Calls
    LEFT JOIN "Models" AS Models ON (Calls."ModelId" = Models."Id")
    LEFT JOIN "Cities" AS Cities ON (Cities."Id" = Calls."CityId")
    WHERE "StartTime" + interval '3 Hour' >= '{0}'
      AND "CallSource" != 'sip'
      AND "StartTime" + interval '3 Hour' < '{1}'
      AND "AnalyticType" != 3
    GROUP BY "CallingPhoneNumber";""".format(dateFrom, dateTo)


def query_customers(days=1, date_format=DATE_FRMT):
    """
    Query to select customers from orders.
    """
    dateFrom = (datetime.today()-timedelta(days)).strftime(date_format)
    dateTo = datetime.today().strftime(date_format)

    return """SELECT min(Orders."Created") AS date,
       Customers."Phone",
       sum(sp."ServiceCost") sum
FROM "ServicesAndParts" AS sp
JOIN "Orders" AS orders ON (sp."OrderId" = orders."Id")
JOIN "OrderStatuses" AS order_statuses ON (orders."OrderStatusId" = order_statuses."Id")
JOIN "ServiceCenters" AS ServiceCenters ON (ServiceCenters."Id" = orders."ServiceCenterId")
JOIN "Customers" AS Customers ON (Customers."Id" = Orders."CustomerId")
WHERE sp."DateTime" >= '2020-05-01'
  AND orders."Posted" = TRUE
  AND Cities."Name" != 'Минск'
  AND ServiceCenters."Id" NOT IN ('df61d005-7925-11e8-810b-005056011ef2',
                                  'd7ef4b0a-ebbd-11e8-8116-005056011ef2')
  AND sp."ServiceCost" > 0
  AND ((order_statuses."TypeId" = 'Закрыт'
        AND orders."Date" >= '{0}'
        AND orders."Date" < '{1}'
        AND sp."IsCancellation" = FALSE)
       OR (sp."IsCancellation" = TRUE
           AND orders."Date" >= '{0}'
           AND orders."Date" < '{1}'))
GROUP BY Customers."Phone",
         Orders."ModelId",
         ServiceCenters."CityId"""".format(dateFrom, dateTo)

def test_query():
    return """
        SELECT * FROM Calls;
    """
