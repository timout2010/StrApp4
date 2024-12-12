// main.js

document.addEventListener('DOMContentLoaded', function () {
  // Configuration Constants
    //const API_URL = 'http://localhost:7190/api/GetPaginatedData';
    //const API_SUB_URL = 'http://localhost:7190/api/GetSubtableData';
    //const TABLE_NAME = 'pocGLcsv';


    const API_URL = window.API_URL;//'http://localhost:7190/api/GetPaginatedData';
    const API_SUB_URL = window.API_SUB_URL;
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
      onSelectionChanged: onMainGridSelectionChanged,
  };

    const subGridOptions = {
        columnDefs: [], // Will be set dynamically
        defaultColDef: {
            flex: 1,
            minWidth: 100,
            resizable: true,
            sortable: true,
            filter: true,
        },
        rowModelType: 'clientSide', // Assuming details are small
        animateRows: true,
        overlayLoadingTemplate: '<span class="custom-loading-overlay">Loading details...</span>',
        overlayNoRowsTemplate: '<span class="custom-loading-overlay">No details to display.</span>',

    };

  const eGridDiv = document.querySelector('#myGrid');
  const gridApi = agGrid.createGrid(eGridDiv, gridOptions);

  fetchColumnsAndInitializeGrid(gridApi);

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
        api.setGridOption("rowSelection", {         mode: 'multiRow'     })
	api.setGridOption("columnDefs",        result.columns.map((col) => ({
          field: col,
          sortable: true,
          filter: true,
        }))
      );

	
      const datasource = createServerSideDatasource(API_URL, TABLE_NAME, PAGE_SIZE);
      api.setGridOption("serverSideDatasource",datasource)
//      api.setServerSideDatasource(datasource);
    } catch (error) {
      console.error('Error initializing :', error);
      alert('Failed  Check console for details.');
    }
  }

  function createServerSideDatasource(apiUrl, tableName, pageSize) {
    return {
      getRows: async function (params) {
        console.log('[Datasource] - rows requested by grid:', params.request);

        const startRow = params.request.startRow;
        const endRow = params.request.endRow;
        const page = Math.floor(startRow / pageSize) + 1;

        try {

          let sortParams = '';
          if (params.request.sortModel && params.request.sortModel.length > 0) {

                    const sortModel = params.request.sortModel[0];
                    sortParams = `&sortBy=${encodeURIComponent(sortModel.colId)}&sortOrder=${encodeURIComponent(sortModel.sort)}`;
          }
	

          const url = `${apiUrl}?tablename=${encodeURIComponent(        tableName
          )}&page=${page}&pagesize=${pageSize}${sortParams}&filter=${FILTER}`;

          const response = await fetch(url);

          if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
          }

          const result = await response.json();

          if (
            !result.columns ||
            !Array.isArray(result.columns) ||
            !result.data ||
            !Array.isArray(result.data) ||
            typeof result.totalRows !== 'number'
          ) {
            throw new Error('Invalid response structure from API.');
          }

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
        } catch (error) {
          console.error('Error fetching :', error);
          params.failCallback();
        }
      },
    };
  }
    // Event handler for main grid selection changes
    async function onMainGridSelectionChanged(event) {
        const selectedRows = event.api.getSelectedRows();
        if (selectedRows.length === 0) {
            // Clear the subtable if no rows are selected
            gridsubApi.setGridOption("rowData", []);
            return;
        }

        // Extract identifiers (e.g., IDs) from selected rows
        const selectedIds = selectedRows.map(row => row.journalid); // Adjust 'id' based on your data
        gridsubApi.showLoadingOverlay()
        // Fetch details for selected IDs
        try {

            const detailsResponse = await fetch(
                `${API_SUB_URL}?tablename=${encodeURIComponent(TABLE_NAME)}&journal_ids=${encodeURIComponent(selectedIds.join(','))}`
            );

            if (!detailsResponse.ok) {
                throw new Error(`HTTP error! Status: ${detailsResponse.status}`);
            }

            const detailsResult = await detailsResponse.json();

            // Validate details response structure
            if (!detailsResult.data || !Array.isArray(detailsResult.data)) {
                throw new Error('Invalid details response structure from API.');
            }

            // Populate the subtable
            gridsubApi.setGridOption("columnDefs", detailsResult.columns.map((col) => ({
                field: col,
                sortable: true,
                filter: true,
            }))
            );
            gridsubApi.setGridOption("rowData", detailsResult.data);

            //subGridOptions.api.setRowData(detailsResult.data);
        } catch (error) {
            console.error('Error fetching subtable data:', error);
            alert('Failed to load subtable data. Check console for details.');
        }
    }



    const eSubGridDiv = document.querySelector('#subGrid');
    const gridsubApi = agGrid.createGrid(eSubGridDiv, subGridOptions);

    
    // Function to fetch and set subtable columns (Assuming API provides it)
    async function initializeSubGridColumns() {
        try {
            // Fetch column definitions for details
            //const response = await fetch(
            //    `${API_SUB_URL}?tablename=${encodeURIComponent(TABLE_NAME)}`
            //);

            //if (!response.ok) {
            //    throw new Error(`HTTP error! Status: ${response.status}`);
            //}

            //const columnsResult = await response.json();

            //if (!columnsResult.columns || !Array.isArray(columnsResult.columns)) {
            //    throw new Error('Invalid columns response structure for subtable.');
            //}

            //subGridOptions.api.setColumnDefs(columnsResult.columns.map(col => ({
            //    field: col,
            //    sortable: true,
            //    filter: true,
            //})));
        } catch (error) {
            console.error('Error initializing subtable columns:', error);
            alert('Failed to initialize the subtable. Check console for details.');
        }
    }

    // Initialize subtable columns on load
    initializeSubGridColumns();

});
