
consistent_order = ['Report_Date','Severity','Region','Country','City','Customer_Hotel_ID','Hotel_Name','Check-In-Date','LOS','Adult','Child','Currency','Tolerance%','Overall_Cheapest_Source','Cheapest_Competitor','W/L/M','Rate/Availability','Variance','Variance%','BM_Rate','BM_Room_Type','BM_Board_Type','BM_Cancellation_Policy','BM_Shop_Time','AAA_Rate','AAA_Room_Type','AAA_Board_Type','AAA_Cancellation_Policy','AAA_Variance(%)','AAA_ShopTime','BAR_Rate','BAR_Room_Type','BAR_Board_Type','BAR_Cancellation_Policy','BAR_Variance(%)','BAR_ShopTime','LOY_Rate','LOY_Room_Type','LOY_Board_Type','LOY_Cancellation_Policy','LOY_Variance(%)','LOY_ShopTime']
expected_rates=[
  {
    "Customer_Hotel_ID": 228101,
    "Check_In_Date": 45478,
    "Rates": {
      "BM_Rate": 255.55,
      "BAR_Rate": 255.55,
      "AAA_Rate": 269,
      "LOY_Rate": 236.72
    }
  },
  {
    "Customer_Hotel_ID": 228101,
    "Check_In_Date": 45508,
    "Rates": {
      "BM_Rate": 236.55,
      "BAR_Rate": 236.55,
      "AAA_Rate": 249,
      "LOY_Rate": 219.12
    }
  },
  {
    "Customer_Hotel_ID": 228102,
    "Check_In_Date": 45479,
    "Rates": {
      "BM_Rate": 245.55,
      "BAR_Rate": 245.55,
      "AAA_Rate": 259,
      "LOY_Rate": 226.72
    }
  },
  {
    "Customer_Hotel_ID": 228103,
    "Check_In_Date": 45480,
    "Rates": {
      "BM_Rate": 265.55,
      "BAR_Rate": 265.55,
      "AAA_Rate": 279,
      "LOY_Rate": 246.72
    }
  }
]
