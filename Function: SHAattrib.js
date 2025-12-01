Function: Node.js SHAAttrib
- Code :
```javascript
const crypto = require('crypto');
const fs = require('fs');

// Lire le contenu du fichier
const fileContent = fs.readFileSync(context.getBinaryData().data, 'utf8');

// Calculer SHA-256
const hash = crypto.createHash('sha256');
hash.update(fileContent);
const sha256 = hash.digest('hex');

// Ajouter à l'item
return [{
    json: {
        ...items[0].json,
        sha256: sha256,
        timestamp: new Date().toISOString()
    }
}];