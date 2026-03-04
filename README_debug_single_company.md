# Single Company Debug Script

## Purpose
Debug person count issues for a specific company using their Prospeo Company ID. This script provides detailed step-by-step debugging output to identify exactly where domain extraction or person search is failing.

## Usage

### Basic Usage
```bash
python3 debug_single_company.py <prospeo_company_id>
```

### With Verbose Output
```bash
python3 debug_single_company.py <prospeo_company_id> --verbose
```

### Help
```bash
python3 debug_single_company.py --help
```

## Example Companies (from Production)

Available prospeo_company_ids you can test with:
- `cccc68bca74898316eb713b7` - PRIMUS Global Services
- `cccc603836b91c82b6e32e74` - Vetsource  
- `ccccf6e180040f9a2af091da` - Pro-Vigil Surveillance Services
- `cccc1ab3ea411fcea31e31c1` - Trinity Logistics
- `ccccccd002edb8cb972b9f9f` - EET Fuels

## What It Does

1. **Finds the company** by prospeo_company_id in the production database
2. **Debugs domain extraction** - Shows exactly which domains are found from website/domain/other_websites
3. **Tests person search** - Tries each domain and shows API responses  
4. **Shows existing records** - Displays previous person count attempts for context

## Sample Output

```
=== DOMAIN EXTRACTION DEBUG ===
Company: PRIMUS Global Services
  Domain: primusglobal.com
  Website: https://primusglobal.com
  Other websites: null
Domain extraction result: ['primusglobal.com']
✅ Domain extraction successful

=== PERSON SEARCH DEBUG ===
Trying domain 1/1: primusglobal.com (from website)
  ✅ Success: 15 people found
🎉 SUCCESS! Found 15 people using website: primusglobal.com

=== FINAL RESULT ===
Company: PRIMUS Global Services
Total count: 15
Status: ok
Successful domain: primusglobal.com (from website)
```

## When to Use This Script

- Investigating "No domains available" errors
- Testing domain extraction logic changes
- Verifying person search API responses
- Debugging specific company failures reported by users
