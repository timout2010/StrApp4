// main.js

document.addEventListener('DOMContentLoaded', function () {
  // Configuration Constants
  const API_URL = window.API_URL ;//'http://localhost:7190/api/GetPaginatedData';
  const TABLE_NAME = window.TABLE_NAME;//'pocGLcsv';
  const FILTER=	window.FILTER ;
  const PAGE_SIZE = 20; // Adjust as needed

  // Grid Options
  const gridOptions = {
    // Initially, columnDefs are empty. They will be set dynamically after fetching from the server.
    columnDefs: [],

    defaultColDef: {
      flex: 1,
      minWidth: 100,
      resizable: true,
      sortable: true,
      filter: true,
    },
    rowModelType: 'serverSide',

    pagination: true,
    paginationPageSize: PAGE_SIZE,
    cacheBlockSize: PAGE_SIZE,
    animateRows: true,
  };

  // Initialize the grid using createGrid and obtain the gridApi
  const eGridDiv = document.querySelector('#myGrid');
  const gridApi = agGrid.createGrid(eGridDiv, gridOptions);

  // After the grid is created, initialize the grid data
  fetchColumnsAndInitializeGrid(gridApi);

  /**
   * Fetch column definitions from the server and initialize the grid.
   * @param {GridApi} api - AG Grid's API instance.
   */
  async function fetchColumnsAndInitializeGrid(api) {
    try {
      // Fetch the first page to get column definitions
      const response = await fetch(
        `${API_URL}?tablename=${encodeURIComponent(TABLE_NAME)}&page=1&pagesize=${PAGE_SIZE}&filter=${FILTER}`
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
     
	api.setGridOption("columnDefs",        result.columns.map((col) => ({
          field: col,
          sortable: true,
          filter: true,
        }))
      );

	
      // Set column definitions based on response
//      api.setColumnDefs(
 //       result.columns.map((col) => ({
  //        field: col,
   //       sortable: true,
    //      filter: true,
     //   }))
      //);

      // Create and set the server-side datasource
      const datasource = createServerSideDatasource(API_URL, TABLE_NAME, PAGE_SIZE);
      api.setGridOption("serverSideDatasource",datasource)
//      api.setServerSideDatasource(datasource);
    } catch (error) {
      console.error('Error initializing grid:', error);
      alert('Failed to load grid data. Check console for details.');
    }
  }

  /**
   * Creates a server-side datasource for AG Grid.
   * @param {string} apiUrl - The API endpoint.
   * @param {string} tableName - The table name parameter.
   * @param {number} pageSize - Number of records per page.
   * @returns {ServerSideDatasource} - The configured datasource.
   */
  function createServerSideDatasource(apiUrl, tableName, pageSize) {
    return {
      /**
       * Fetches rows from the server based on AG Grid's request.
       * @param {IServerSideGetRowsParams} params - Parameters from AG Grid.
       */
      getRows: async function (params) {
        console.log('[Datasource] - rows requested by grid:', params.request);

        // Calculate the page number
        const startRow = params.request.startRow;
        const endRow = params.request.endRow;
        const page = Math.floor(startRow / pageSize) + 1;

        try {

          let sortParams = '';
          if (params.request.sortModel && params.request.sortModel.length > 0) {
                    // Assuming single column sorting. For multiple, adjust accordingly.
                    const sortModel = params.request.sortModel[0];
                    sortParams = `&sortBy=${encodeURIComponent(sortModel.colId)}&sortOrder=${encodeURIComponent(sortModel.sort)}`;
          }
	
          // Construct the API URL with query parameters


          const url = `${apiUrl}?tablename=${encodeURIComponent(        tableName
          )}&page=${page}&pagesize=${pageSize}${sortParams}&filter=${FILTER}`;

          const response = await fetch(url);

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

          // Optionally, update columnDefs if columns might change dynamically
          // Uncomment the following lines if your columns can change based on data
          /*
          api.setColumnDefs(
            result.columns.map((col) => ({
              field: col,
              sortable: true,
              filter: true,
            }))
          );
          */
	          params.success({ rowData: result.data ,rowCount:result.totalRows});	
          // Pass the rows to AG Grid
//          params.successCallback(result.data, result.totalRows);
        } catch (error) {
          console.error('Error fetching rows:', error);
          params.failCallback();
        }
      },
    };
  }
});
