// main.js

document.addEventListener('DOMContentLoaded', function () {
  // Configuration Constants
   // const API_URL = 'http://localhost:7190/api/GetPaginatedData';
   // const API_SUB_URL = 'http://localhost:7190/api/GetSubtableData';
    //const TABLE_NAME = 'pocGLcsv';
   // const API_PIVOT_URL = 'http://localhost:7190/api/RunSQL';


    const API_URL = window.API_URL;//'http://localhost:7190/api/GetPaginatedData';
    const API_SUB_URL = window.API_SUB_URL;
    const API_PIVOT_URL = window.API_PIVOT_URL;
    const TABLE_NAME = window.TABLE_NAME;//'pocGLcsv';
    
  const FILTER=	window.FILTER ;
  const PAGE_SIZE = 20; // Adjust as needed

  // Grid Options




    //PivotTable


    

    const gridPivotOptions = {
        defaultColDef: {
            flex: 1,
            minWidth: 150,
            // restrict what aggregation functions the columns can have,
            // include a custom function 'random' that just returns a
            // random number
            allowedAggFuncs: ["sum", "min", "max"],
            filter: true,
        },
        autoGroupColumnDef: {
            width: 180,
        },
        /*columnDefs: columnDefs,*/
        rowModelType: "serverSide",
        rowGroupPanelShow: "always",
        pivotPanelShow: "always",
        sideBar: true,
	pivotRowTotals:"after",
        maxConcurrentDatasourceRequests: 1,
        maxBlocksInCache: 2,
	pivotMode:true,
        purgeClosedRowNodes: true,
    };


    const ePivotGridDiv = document.querySelector('#myPivotGrid');
    const gridPivotApi = agGrid.createGrid(ePivotGridDiv, gridPivotOptions);

    //fetch(`${API_SUB_URL}?tablename=${encodeURIComponent(TABLE_NAME)}`)
    //    .then((response) => response.json())
    //    .then(function (data) {
    //        const datasource = createPivotServerSideDatasource(API_PIVOT_URL, TABLE_NAME);
          
    //        gridPivotApi.setGridOption("serverSideDatasource", datasource);
    //    });

    fetchPivotColumnsAndInitializeGrid(gridPivotApi);

    async function fetchPivotColumnsAndInitializeGrid(api) {
        try {
            // Fetch the first page to get column definitions
            const response = await fetch(
                `${API_SUB_URL}?tablename=${encodeURIComponent(TABLE_NAME)}`
            );

            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }

            const result = await response.json();

            // Validate response structure
            if (
                !result.columns ||
                !Array.isArray(result.columns) ||
                !result.data ||
                !Array.isArray(result.data) ||
                typeof result.totalRows !== 'number'
            ) {
                throw new Error('Invalid response structure from API.');
            }
            api.setGridOption("rowSelection", { mode: 'multiRow' })
            api.setGridOption("columnDefs", result.columns.map((col) => ({
                field: col,
                sortable: true,
                filter: true,
                enableRowGroup: true,
                enablePivot: true,
                rowGroup: false,
                hide: false,
                
                enableValue: true,
            }))
            );


            const datasource = createPivotServerSideDatasource(API_PIVOT_URL, TABLE_NAME,api);
            api.setGridOption("serverSideDatasource", datasource)
            //      api.setServerSideDatasource(datasource);
        } catch (error) {
            console.error('Error initializing :', error);
            alert('Failed  Check console for details.');
        }
    }

    function createPivotServerSideDatasource(apiUrl, tableName,api) {
        return {
            getRows: async function (params) {
                console.log('[Datasource] - rows requested by grid:', params.request);

                
                try {

                    let sortParams = '';
                    const payload = {
                        tableName: tableName,    // or simply `tableName,`
                        requestModel: params.request,

                    };
                    const fetchOptions = {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            // Add other headers if necessary, e.g., Authorization
                        },
                        body: JSON.stringify(payload)
                    };

                    const url = `${apiUrl}`;

                    const response = await fetch(url, fetchOptions);

                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }

                    const result = await response.json();

                    if (!result.rowData || !Array.isArray(result.rowData) || typeof result.rowCount !== 'number') {
                        throw new Error('Invalid response structure from API.');
                    }

                    //if (!result.rowData || result.rowData.length === 0) {
                    //    api.setGridOption("columnDefs", []);
                    //}
                    //else {
                    //    const rowData = result.rowData[0];


                    //    const fieldNames = Object.keys(rowData);
                    //    api.setGridOption("columnDefs", fieldNames.map((col) => ({
                    //        field: col,
                    //        sortable: true,
                    //        filter: true,
                    //        enableRowGroup: true,
                    //        enablePivot: true,
                    //        rowGroup: false,
                    //        hide: false,

                    //        enableValue: true,
                    //    }))
                    //    );
                    //}
                    

                    /*
                    api.setColumnDefs(
                      result.columns.map((col) => ({
                        field: col,
                        sortable: true,
                        filter: true,
                      }))
                    );
                    */
                    params.success({ rowData: result.rowData, rowCount: result.rowCount, pivotResultFields: result.pivotResultFields });
                } catch (error) {
                    console.error('Error fetching :', error);
                    params.failCallback();
                }
            },
        };
    }

});
