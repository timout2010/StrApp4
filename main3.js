// main.js      
document.addEventListener('DOMContentLoaded', initializeGrid);

const CONFIG = {
    API_URL: window.API_URL,
    API_SUB_URL: window.API_SUB_URL,
    TABLE_NAME: window.TABLE_NAME,
    API_PIVOT_URL: window.API_PIVOT_URL,
    API_PIVOTCHART_URL: window.API_PIVOTCHART_URL,
    PAGE_SIZE: 20, // Number of rows per page
    GRID_CONTAINER_ID: '#myPivotChartGrid',
    CHART_CONTAINER_ID: '#columnChart',
};

// Grid API reference
let gridPivotChartApi = null;

/**
 * Initializes the ag-Grid with the specified configurations.
 */
function initializeGrid() {
    const gridOptions = getGridOptions();
    const gridDiv = document.querySelector(CONFIG.GRID_CONTAINER_ID);
    agGrid.createGrid(gridDiv, gridOptions);
}

/**
 * Returns the grid options configuration for ag-Grid.
 */
function getGridOptions() {
    return {
        columnDefs: getColumnDefs(),
        defaultColDef: getDefaultColDef(),
        rowModelType: "clientSide", // Changed to "clientSide" for client-side row model


 defaultColDef: {
    flex: 1,
    editable: true,
    filter: "agMultiColumnFilter",
    floatingFilter: true,
  },
  enableCharts: true,
  chartThemeOverrides: {
    bar: {
      axes: {
        category: {
          label: {
            rotation: 0,
          },
        },
      },
    },
  },        onFirstDataRendered: onFirstDataRendered,
        onGridReady: onGridReady,
    };
}

/**
 * Defines the column configurations for the grid.
 */
function getColumnDefs() {
    return [
        { 
            field: "glAccountNumber", 
            chartDataType: "category",
            sortable: true,
            filter: "agSetColumnFilter",
            editable: false, // Making column non-editable
        },
        { 
            field: "risk_label", 
            chartDataType: "category"   ,
            filter: "agSetColumnFilter",
        },
        {
            field: "risk_count",
            maxWidth: 160,
            aggFunc: "sum",
            filter: "agNumberColumnFilter",
            chartDataType: "series",
            sortable: true,
            editable: true, // This column remains editable
        },
        {
            field: "month",
            maxWidth: 160,
            filter: "agSetColumnFilter",
            chartDataType: "category",
            sortable: true,
            editable: false, // Making column non-editable
        },
    ];
}

/**
 * Defines the default column definitions.
 */
function getDefaultColDef() {
    return {
        flex: 1,
        editable: true,
        filter: "agMultiColumnFilter",
        floatingFilter: true,
        sortable: true, // Enable sorting by default
    };
}

/**
 * Callback when the grid is ready.
 * Fetches all data and populates the grid.
 */
function onGridReady(params) {
    gridPivotChartApi = params.api;
    fetchAllDataAndInitializeGrid(params.api);
}

/**
 * Callback when the first data is rendered.
 * Initializes the chart.
 */
function onFirstDataRendered(params) {
    createQuarterlySalesChart(params.api);
    // You can uncomment and implement additional charts as needed
     createSalesByRefChart(params.api);
    createHandsetSalesChart(params.api);
}

/**
 * Fetches all data from the API and sets it to the grid.
 * Uses the client-side row model.
 * @param {GridApi} api - The ag-Grid API instance.
 */
async function fetchAllDataAndInitializeGrid(api) {
    try {
        showLoadingOverlay(api);
	const url = `${CONFIG.API_PIVOTCHART_URL}?chart=chart5`;



        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                // Add other headers if necessary, e.g., Authorization
            },
        });

        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const data = await response.json();
        console.log('Fetched Data:', data);

        // Ensure data is an array
        if (!Array.isArray(data)) {
            throw new Error('Expected data to be an array');
        }

        api.setGridOption("rowData",data); // Populate grid with fetched data
    } catch (error) {
        console.error('Error fetching data:', error);
        alert('Failed to load data. Check console for details.');
    } finally {
        hideLoadingOverlay(api);
    }
}

/**
 * Creates a column chart based on the grid data.
 * @param {GridApi} api - The ag-Grid API instance.
 */
function createQuarterlySalesChart(api) {
  api.createCrossFilterChart({
    chartType: "column",
    cellRange: {
      columns: [ "month","risk_count"],
    },
    aggFunc: "sum",
    
    chartContainer: document.querySelector("#columnChart"),
  });
}


function createSalesByRefChart(api) {
  api.createCrossFilterChart({
    chartType: "pie",
    cellRange: {
      columns: ["risk_label", "risk_count"],
    },
    aggFunc: "sum",
    chartThemeOverrides: {
      common: {
        title: {
          enabled: true,
          text: "Sales by Representative ($)",
        },
      },
      pie: {
        series: {
          title: {
            enabled: false,
          },
          calloutLabel: {
            enabled: false,
          },
        },
        legend: {
          position: "right",
        },
      },
    },
    chartContainer: document.querySelector("#pieChart"),
  });
}


function createHandsetSalesChart(api) {
  api.createCrossFilterChart({
    chartType: "groupedColumn",
    cellRange: {
      columns: ["glAccountNumber", "risk_count"],
    },
    aggFunc: "count",
    chartThemeOverrides: {
      common: {
        title: {
          enabled: true,
          text: "--",
        },
        legend: { enabled: false },
      },
    },
    chartContainer: document.querySelector("#barChart"),
  });
}

/**
 * Displays a loading overlay on the grid.
 * @param {GridApi} api - The ag-Grid API instance.
 */
function showLoadingOverlay(api) {
//    api.showLoadingOverlay();
}

/**
 * Hides the loading overlay on the grid.
 * @param {GridApi} api - The ag-Grid API instance.
 */
function hideLoadingOverlay(api) {
  //  api.hideOverlay();
}

/**
 * (Optional) Creates additional charts based on grid data.
 * Implement similar to createQuarterlySalesChart as needed.
 */
/*
function createSalesByRefChart(api) {
    api.createCrossFilterChart({
        chartType: "bar",
        cellRange: {
            columns: ["salesByRef"],
        },
        aggFunc: "sum",
        chartContainer: document.querySelector("#salesByRefChart"),
    });
}

function createHandsetSalesChart(api) {
    api.createCrossFilterChart({
        chartType: "pie",
        cellRange: {
            columns: ["handsetSales"],
        },
        aggFunc: "sum",
        chartContainer: document.querySelector("#handsetSalesChart"),
    });
}
*/
