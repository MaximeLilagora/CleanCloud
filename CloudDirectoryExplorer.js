// Nœud : Cloud Directory Explorer
{
  "name": "Cloud Directory Explorer",
  "parameters": {
    "functionCode": `
      // Structure d'exploration
      const directoryStructure = {
        root_path: '/path/to/cloud',
        scan_date: new Date().toISOString(),
        total_files: 0,
        total_directories: 0,
        file_types: {},
        size_statistics: {
          total_size: 0,
          largest_file: null,
          smallest_file: null
        }
      };
      

      return items.map((item, index) => {
        const fileData = {
          file_path: item.json.file_path || item.json.path,
          file_name: item.json.file_name || item.json.name,
          file_size: item.json.size || 0,
          file_type: item.json.file_type || 'unknown',
          last_modified: item.json.last_modified || new Date().toISOString(),
          is_directory: item.json.is_directory || false,
          cloud_provider: 'your_cloud_provider',
          scan_id: 'scan_' + Date.now() + '_' + index,
          status: 'explored'
        };
        
        return {
          json: fileData
        };
      });
    `
  }
}