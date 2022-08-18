# MENAT Pathways - Phase 1

<details open="open">

## Table of Contents

1.  [Objective](#objective)
2.  [Sources](#sources)
3.  [Platform/Toolsets/Technologies](#platformtoolsetstechnologies)
4.  [Output](#output)
5.  [Data Flow](#data-flow)
6.  [Process Flow](#process-flow)
7.  [Components Flow](#components-flow)
8.  [File Inventory](#file-inventory)
9.  [Hive Inventory](#table-inventory)
10.  [Key Pointers](#key-pointers)
11. [Manual Override](#manual-override)
12. [Web Portal](#web-portal)

- [Technical Details](#technical-details)
- [Web Portal Deployment](#web-portal-deployment)

13. [Business Contact](#business-contact)
14. [RFC](#rfc)
15. [Planview](#planview)
16. [Project folder in Development](#project-folder-in-development)
17. [Project folder in Production](#project-folder-in-production)
18. [Troubleshooting](#troubleshooting)
19. [NiFi](#nifi)
20. [BODS](#bods)
21. [Enhancement](#enhancement---1)

</details>

## Objective

The objective of this project is generate Shipment forecast for MENAT by Distributor/SKU. The forecast is generated for 18months starting from Jan of every year. Some of the key requirements for Phase 1 include:

- Generate Shipment forecast by Distributor(Customer in SAP), SKU and Period
- SKUs are from EAP and KOP
- Actual Shipment report
- Planned Visibility Report
- Snapshot of Forecast cycles (0+12, 1+11 etc)
- Forecast in Cases/KGs/Tons
- Upload IMS Actual
- Upload IMS Forecast
- Upload Opening Stock
- Maintain master data - Lead time, Transit time and DFU Mapping

## Sources

The key data sources for this project are

- SAP EAP
- SAP KOP
- IMS Data upload via Portal
- Master data mappings
- Logistic file

## Platform/Toolsets/Technologies

- Keystone 1.0 as Analytics platform
- Apache NiFi for process orchestration
- HDFS for data storage
- Hive for Virtual DW
- Python for data processing
- Shell scripts as components wrapper
- Tableau for reporting
- Python-Flask Web Application for data upload

## Output

As part of Phase-1, Integrated Pipeline Report will be generated on Excel and Tableau. The excel version will be sent via email and Tableau report is for historical comparison.

## Data Flow

![image](https://user-images.githubusercontent.com/111518974/185426150-dbe8d0f0-91b6-4ad7-9ce7-e9b9da47a19b.png)

## Process Flow

![image](https://user-images.githubusercontent.com/111518974/185426068-87d9707a-a3f9-42e7-9902-f0b69984f856.png)

### Step - 1

Extract Orders, Invoices, Customer, Material, Plant data from EAP and KOP via SAP Data Services and generate the output files in usoak044. This flow is existing and was developed as part of AMEA Data Lake project. Frequency of this process is daily.

### Step - 2

Maintain/Update mapping files available on usoak044. Frequency of this process is adhoc.

### Step - 3

Load SAP/Mapping data to HDFS via NiFi. SAP data is loaded via existing BU Data Lake PG. Mapping files are loaded via NiFi PG created for this project. Frequency is based on Step-1/Step-2 triggers.

### Step - 4

Generate and Export Shipment/Planned Visibility report to usoak044. There is a shell script that will export data from Hive and save the results. This script will run once a day.

### Step - 5

File generated in Setp-4 will be downloaded by user using Web Portal.

### Step - 6

User will upload IMS Actual, Forecast, OS and Shipment confirmation file vi web portal

### Step - 7

Uploaded files in Step-6 will be saved to usoak044

### Step - 8

Once files from Step-7 are saved to usoak044, NiFi PG will be trigerred and the data will be copied to HDFS

### Step - 9

Data from Hive - IMS Actual, Forecast, OS and SAP Orders/Invoices for current cycle will be downloaded from Hive to Local FS. These files will be used as input in Step - 10.

### Step - 10

This is the where main data transformation for Pipeline report happens. Uploaded data (IMS) and SAP Data(Orders/Invoices) are merged and transformed to generate final Pipeline report. The generated excel will be sent via email to the user via NiFi.

- Merge SAP arrivals and orders
- rename col in leftover file
- Convert SKU from SAP to DFU
- Consider forecast data for >= Current Period
- Consider actuals data for < Current Period
- Using actuals,forecast and OS data, use DFU mapping to get new DFU
- Consider OS for Current Period
- Calculate adjustment for past periods
- Calculate leftovers and add to arrivals of current period
- Get conversion factor,category,brand and stock cover days from mapping files.
- Calculate WOC
- Calculate suggested orders
- Generate intermediate output and execute Excel Transformer to create final formatted excel output.

### Step - 11

Final Pipeline report output is copied to HDFS for Tableau reporting.

## Components Flow

![ComponentsFlow](/images/5142c877509e6cb0784ef0d77062b1c5.png)

## Integrated Pipeline Report (IPR)

![IPR](/images/740a6bde3d048675c78e21ab10424ac7.png)

## File Inventory

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| #   | Name | Source | File Name | Business Input | Notes |
| 1   | IMS Actual | Web Portal | 1\_MPW\_IMS\_Actual\_IN_&lt;Distributor&gt;_&lt;Date&gt;.csv | Y   |     |
| 2   | IMS Forecast | Web Portal | 2\_MPW\_IMS\_Forecast\_IN_&lt;Distributor&gt;_&lt;Date&gt;.csv | Y   |     |
| 3   | IMS Stock | Web Portal | 3\_MPW\_IMS\_OS\_IN_&lt;Distributor&gt;_&lt;Date&gt;.csv | Y   |     |
| 4   | Web Confirmation | Web Portal | 4\_MPW\_IMS\_WEB\_IN_&lt;Distributor&gt;_&lt;Date&gt;.csv | Y   |     |
| 5   | Export Doc | Manual | 5\_MPW\_KSOP\_ExportDoc\_IN.csv | Y   |     |
| 6   | Planned Visibiilty | Hive | 6\_MPW\_PlannedVisibility_OUT.csv |     |     |
| 7   | Actual Shipment | Hive | 7\_MPW\_ActualShipment_OUT.csv |     |     |
| 8   | Pipeline Ouput from Python | Python Script | 8\_MPW\_Pipeline\_Python\_&lt;Version&gt;_&lt;Period&gt;.csv |     |     |
| 9   | Pipeline Output Excel Version | Python Script | 9\_MPW\_IntegratedPipeline_&lt;Version&gt;_&lt;Period&gt;.csv |     |     |
| 10  | DFU Mapping | Manual | 10\_MPW\_DFUMapping_IN.csv | Y   | Old,New DFU and Weight with Brand/Category |
| 11  | STP Mapping | Manual | 11\_MPW\_STPMapping_IN.csv | Y   | Cluster,Region information |
| 12  | Plant Lead time | Manual | 12\_MPW\_PlantLeadTime_IN.csv | Y   |     |
| 13  | Plant Transit time | Manual | 13\_MPW\_PlantTransitTime_IN.csv | Y   |     |
| 14  | PresentCycle | Manual | 14\_MPW\_CyclePeriod.csv |     |     |
| 15  | IMS Actual | Hive | 15\_MPW\_IMS\_Actual\_OUT.csv |     |     |
| 16  | IMS Forecast | Hive | 16\_MPW\_IMS\_Forecast\_OUT.csv |     |     |
| 17  | IMS Stock | Hive | 17\_MPW\_IMS\_OS\_OUT.csv |     |     |
| 18  | Web Confirmation | Hive | 18\_MPW\_IMS\_WEB\_OUT.csv |     |     |
| 19  | SAP Arrivals Consolidated | Python Intermediate Output | 19\_MPW\_SAPArrivals_OUT.csv |     |     |
| 20  | Pipieline LM | Hive | 20\_MPW\_PipelineLM_OUT.csv |     |     |
| 21  | Stock Cover Days | Manual | 21\_MPW\_StockCoverDays_IN.csv | Y   |     |
| 22  | SAP Arrivals Orders | Hive | 22\_MPW\_SAP\_Arrivals\_Orders_OUT.csv |     |     |
| 23  | SAP Arrivals Invoices | Hive | 23\_MPW\_SAP\_Arrivals\_Invoices_OUT.csv |     |     |
| 24  | ADJ | Hive | 24\_MPW\_IMS\_ADJ\_OUT.csv |     |     |
| 25  | ADJ of Last Month Python output | Python Script | 25\_MPW\_ADJ\_LM\_Python_&lt;Version&gt;_&lt;Period&gt;.csv |     |     |
| 26  | Month mapping for Pipeline Output | Manual | 26\_MPW\_Month\_Mapping\_IPR.csv |     |     |
| 27  | Left Overs | Hive | 27\_MPW\_Leftovers_OUT.csv |     |     |
| 28  | Left Overs | Python Script | 28\_MPW\_Leftovers_IN.csv |     |     |
| 29  | Debug File 1 | Python Script | 29\_MPW\_ExcelTransInputDebug.csv |     |     |
| 30  | Debug File 2 | Python Script | 30\_MPW\_DebugFileAfterJoinActualForecastOSArrival.csv |     |     |
| 31  | Debug File 3 | Python Script | 31\_MPW\_DebugFileAfterJoinAdjustment.csv |     |     |
| 32  | Debug File 4 | Python Script | 32\_MPW\_DebugFileCurrArrivalLeftover.csv |     |     |
| 33  | Portal Analytics | Portal | 33\_MPW\_PortalInfo |     |     |

## Table Inventory

|     |     |
| --- | --- |
| Table Name | Description |
| archive\_ext\_txt\_menat\_pathway\_adj\_stock | archive table for Adjustment stock data. |
| archive\_ext\_txt\_menat\_pathway\_ims\_actual | archive table for IMS Actual data. |
| archive\_ext\_txt\_menat\_pathway\_ims\_forecast | archive table for IMS Forecast data. |
| archive\_ext\_txt\_menat\_pathway_os | archive table for Opening stock. |
| archive\_ext\_txt\_menat\_pathway\_web\_arrivals | archive table for Web Confrmation |
| pcd\_ext\_orc\_menat\_pathway\_adj\_stock | Historical updated records for Adjusted stock data |
| pcd\_ext\_orc\_menat\_pathway\_drm\_pipelineinput | Historical updated processed records for DRM Pipeline report -Input to tableau |
| pcd\_ext\_orc\_menat\_pathway\_ims\_actual | Historical updated processed records for IMA Actal |
| pcd\_ext\_orc\_menat\_pathway\_ims\_forecast | Historical updated processed records for IMS Forecast |
| pcd\_ext\_orc\_menat\_pathway_os | Historical updated processed records forStock |
| pcd\_ext\_orc\_menat\_pathway\_web\_shipment_arrivals | Historical updated processed records for web confirmation |
| src\_ext\_txt\_menat\_pathway\_adj\_stock | Landing table to store latest records for adjustment stock, generated by python |
| src\_ext\_txt\_menat\_pathway\_drm\_pipelineinput | Landing table to store latest records for which DRM Pipeline is executed, records records generated by Python |
| src\_ext\_txt\_menat\_pathway\_ims\_actual | Landing table to store latest records for IMS Actual |
| src\_ext\_txt\_menat\_pathway\_ims\_forecast | Landing table to store latest records for IMS Forecast |
| src\_ext\_txt\_menat\_pathway_os | Landing table to store latest records for OS |
| src\_ext\_txt\_menat\_pathway\_web\_arrivals | Landing table to store latest records forWeb Arrivals |
| src\_ext\_txt\_menat\_pathways\_arrivals\_transit_mapping | Landing table to  store mapping of lead and transit time |
| src\_ext\_txt\_menat\_pathways\_dfu\_mapping | Landing table to store DFU Mapping |
| src\_ext\_txt\_menat\_pathways\_stp\_mapping | Landing table to store STP Mapping |
| uvw\_ext\_orc\_menat\_pathway\_web\_arrival_confirmation | View created to fetch only the confirmation pcd\_ext\_orc\_menat\_pathway\_web\_shipment_arrivals table |
| uvw\_ext\_orc\_menat\_pathway\_web\_arrival\_no\_confirmation | View created to fetch only the non confirmed records by distributor from pcd\_ext\_orc\_menat\_pathway\_web\_shipment_arrivals table |
| uvw\_menat\_pathways\_eap\_actual_shipments | View Created to fetch Invoiced orders from EAP tables for the purpose of Actual Shipment report |
| uvw\_menat\_pathways\_eap\_invoices | View Created to fetch Invoiced orders from EAP tables for the purpose of shipment data input to Python code |
| uvw\_menat\_pathways\_eap\_orders | View Created to fetch open  orders from EAP tables for the purpose of shipment data input to Python code |
| uvw\_menat\_pathways\_eap\_planned_shipments | View Created to fetch open  orders from EAP tables for the purpose of Planned Shipment report |
| uvw\_menat\_pathways\_kop\_actual_shipments | View Created to fetch Invoiced orders from KOP tables for the purpose of Actual Shipment report |
| uvw\_menat\_pathways\_kop\_copa_anz |     |
| uvw\_menat\_pathways\_kop\_copa_base |     |
| uvw\_menat\_pathways\_kop\_copa_prg |     |
| uvw\_menat\_pathways\_kop\_invoices | View Created to fetch Invoiced orders from KOP  tables for the purpose of shipment data input to Python code |
| uvw\_menat\_pathways\_kop\_orders | View Created to fetch open orders from KOP tables for the purpose of shipment data input to Python code |
| uvw\_menat\_pathways\_kop\_planned_shipments | View Created to fetch open  orders from KOP  tables for the purpose of Planned Shipment report |

## Key Pointers

- Data from EAP will be in DFU format with exception of plants 5919,5920,6900,6902,6903.
- Data from KOP will be in SKU format.
- IMS Actual, Forecast and OS will be in DFU format.
- Old to New DFU mapping is maintained in 10\_MPW\_DFUMapping_IN.csv.
- Bill To for each Distributor maintained in 11\_MPW\_STPMapping_IN.csv.
- All transactional data is based on 445 calendar.
- Stock cover days for every distributor will be maintained in 21\_MPW\_StockCoverDays_IN.csv.
- IPR is dynamically designed to process data for N months where N=18 as per initial design. Report is generated from Jan to (Jan + N-1) months.
- Distributor = Customer = BillTo.
- SAP transactions will have SoldTo, BillTo to SoldTo is mapped via Customer Master.
- Suggested orders are calculated for periods when there are no more orders available from SAP

## Manual Override

If there will arise a scenario where IPR needed to be processed for any past month other than current fiscal cycle, update 14\_MPW\_CyclePeriod.csv file with period for which processing is required and date with current date.

## Web Portal

The web portal is developed using Python-Flask and served via WSGI on port 8081. The primary functions of the portal are:

- Allow user to upload IMS Actual, Forecast and OS
- Allow user to upload shipment confirmation
- Allow user to download Actual/Planned shipment report
- Allow user to lock DRM/KSOP versions

The data uploaded via portal are saved to usoak044 and NiFi will process further into HDFS.



### Web Portal Deployment

- Install Nginx
- Install NSSM
- `nssm install nginx` to install Nginx as service
- Install Python 3.7
- Install VC++ 14.0
- Install dependencies via requirements.txt
- Set parameters in .env and aad.config.json
- Adding Python folder path and the Scripts folder path as both User variable and System Variable Eg: C:\\Python37 is the Python folder path and C:\\Python37\\Scripts is the Scripts path.
- Copy the file pywintypes.dll from pywin32_system32, which is under site-packages folder, to win32 folder.
- `python create_service.py --startup auto install` to install MENAT Portal as service
- `python create_service.py debug` for debugging the service



## Project folder in development

```
/home/inca1n04/Project/KAMEA/Bin/
```

## Project folder in development

```
/home/nfadmin/projects/amea/menat_ksop
```

## Troubleshooting

### Pipeline report not generated

- check the log files to see if any process has failed

### Incorrect data in Pipeline report

- check the mapping files and make sure STP/DFU entry is maintained
- check SAP jobs to make sure latest data is pulled into Keystone
- check data in SAP related tables/views on Hive
- check the intermediate output generated for excel generation to see if the data is flowing till final layer
- check the debug files to make sure input data for data transformation is correct

## NiFi

```
NiFi Flow » Playground » AMEA_Test » AMEA_MENAT_KSOP » KAMEA
```

