#!/usr/bin/env python3
"""
Debug script to test HTML parsing on your actual Transfermarkt data
"""

from bs4 import BeautifulSoup

# Your actual HTML data
html_content = '''
<table class="items">
<thead>
<tr>
<th colspan="2" id="yw1_c0">Club</th><th class="hide" id="yw1_c1"><a class="sort-link" href="/2de-nationale-vv-a/startseite/wettbewerb/VFVA/saison_id/2019/plus//sort/name">name</a></th><th class="zentriert" id="yw1_c2"><a class="sort-link" href="/2de-nationale-vv-a/startseite/wettbewerb/VFVA/saison_id/2019/plus//sort/anzahl_spieler_hidden.desc">Squad</a></th><th class="zentriert" id="yw1_c3"><a class="sort-link" href="/2de-nationale-vv-a/startseite/wettbewerb/VFVA/saison_id/2019/plus//sort/alter_durchschnitt.desc">&oslash; age</a></th><th class="zentriert" id="yw1_c4"><a class="sort-link" href="/2de-nationale-vv-a/startseite/wettbewerb/VFVA/saison_id/2019/plus//sort/legionaere.desc">Foreigners</a></th></tr>
</thead>
<tfoot>
<tr>
<td colspan="2">&nbsp;</td><td class="hide">&nbsp;</td><td class="zentriert">425</td><td class="zentriert">24.8 Years</td><td class="zentriert">48</td></tr>
</tfoot>
<tbody>
<tr class="odd">
<td class="zentriert no-border-rechts"><a title="KSK Ronse (-2022)" href="/ksk-ronse/startseite/verein/3736/saison_id/2019"><img src="https://tmssl.akamaized.net//images/wappen/tiny/3736.png?lm=1449922184" title="KSK Ronse (-2022)" alt="KSK Ronse (-2022)" class="tiny_wappen" /></a></td><td class="hauptlink no-border-links"><a title="KSK Ronse (-2022)" href="/ksk-ronse/startseite/verein/3736/saison_id/2019">KSK Ronse (-2022)</a> </td><td class="zentriert"><a title="KSK Ronse (-2022)" href="/ksk-ronse-2022-/kader/verein/3736/saison_id/2019">34</a></td><td class="zentriert">25.5</td><td class="zentriert">11</td></tr>
<tr class="even">
<td class="zentriert no-border-rechts"><a title="FC Gullegem" href="/fc-gullegem/startseite/verein/36414/saison_id/2019"><img src="https://tmssl.akamaized.net//images/wappen/tiny/36414.png?lm=1408615371" title="FC Gullegem" alt="FC Gullegem" class="tiny_wappen" /></a></td><td class="hauptlink no-border-links"><a title="FC Gullegem" href="/fc-gullegem/startseite/verein/36414/saison_id/2019">FC Gullegem</a> </td><td class="zentriert"><a title="FC Gullegem" href="/fc-gullegem/kader/verein/36414/saison_id/2019">24</a></td><td class="zentriert">24.9</td><td class="zentriert">0</td></tr>
<tr class="odd">
<td class="zentriert no-border-rechts"><a title="KFC Mandel United" href="/kfc-mandel-united/startseite/verein/25032/saison_id/2019"><img src="https://tmssl.akamaized.net//images/wappen/tiny/25032_1625162922.png?lm=1625162922" title=" " alt="KFC Mandel United" /></a></td><td class="hauptlink no-border-links"><a title="KFC Mandel United" href="/kfc-mandel-united/startseite/verein/25032/saison_id/2019">KFC Mandel United</a> </td><td class="zentriert"><a title="KFC Mandel United" href="/kfc-mandel-united/kader/verein/25032/saison_id/2019">23</a></td><td class="zentriert">24.9</td><td class="zentriert">2</td></tr>
</tbody>
</table>
'''

def debug_table_parsing():
    """Debug the table parsing step by step - EXTENDED VERSION"""
    
    print("🔍 DEBUGGING TRANSFERMARKT TABLE PARSING - EXTENDED")
    print("=" * 50)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Step 1: Find the table
    print("\n1️⃣ FINDING TABLE:")
    table = soup.find('table', class_='items')
    print(f"   ✅ Table found: {table is not None}")
    
    if not table:
        print("   ❌ FAIL: No table with class 'items' found!")
        return
    
    # Step 2: Get rows
    print("\n2️⃣ FINDING ROWS:")
    tbody = table.find('tbody') or table
    rows = tbody.find_all('tr')
    print(f"   ✅ Found {len(rows)} total rows")
    
    # Step 3: Filter out header/footer rows
    print("\n3️⃣ FILTERING ROWS:")
    data_rows = []
    for i, row in enumerate(rows):
        # Check for header (th elements)
        has_headers = row.find('th') is not None
        
        # Check for footer classes
        row_classes = row.get('class', [])
        is_footer = any('foot' in cls for cls in row_classes)
        
        if has_headers:
            print(f"   ⏭️ Row {i}: SKIPPED (header row)")
        elif is_footer:
            print(f"   ⏭️ Row {i}: SKIPPED (footer row)")
        else:
            data_rows.append(row)
            print(f"   ✅ Row {i}: KEPT (data row)")
    
    print(f"\n   📊 Data rows after filtering: {len(data_rows)}")
    
    # Step 4: Process each data row - DETAILED DEBUGGING
    print("\n4️⃣ PROCESSING DATA ROWS - DETAILED:")
    CLUB_URL_PATTERN = "/startseite/verein/"
    CLUB_TABLE_MIN_COLUMNS = 7  # From your config
    
    for i, row in enumerate(data_rows):
        print(f"\n   --- ROW {i+1} DETAILED ANALYSIS ---")
        
        # Get cells
        cells = row.find_all('td')
        print(f"   📝 Cells found: {len(cells)}")
        
        # CRITICAL: Check minimum column validation
        if len(cells) < CLUB_TABLE_MIN_COLUMNS:
            print(f"   ❌ COLUMN COUNT FAIL: Need {CLUB_TABLE_MIN_COLUMNS}+, got {len(cells)}")
            print(f"   🚨 THIS WOULD BE SKIPPED BY YOUR SCRAPER!")
            continue
        else:
            print(f"   ✅ Column count OK: {len(cells)} >= {CLUB_TABLE_MIN_COLUMNS}")
        
        # Look for club info in first 2 cells
        club_info_found = False
        for cell_idx in range(min(2, len(cells))):
            cell = cells[cell_idx]
            
            # Find club link
            club_link = cell.find('a', href=True)
            if club_link:
                href = club_link.get('href', '')
                club_name = club_link.get_text(strip=True)
                club_title = club_link.get('title', '')
                
                print(f"   🔗 Cell {cell_idx} link: {href}")
                print(f"   📛 Cell {cell_idx} text: '{club_name}'")
                print(f"   🏷️ Cell {cell_idx} title: '{club_title}'")
                
                # Check URL pattern
                if CLUB_URL_PATTERN in str(href):
                    print(f"   ✅ URL pattern match: {CLUB_URL_PATTERN} found in {href}")
                    
                    # CRITICAL: Check if name extraction would fail
                    if not club_name:
                        print(f"   ⚠️ EMPTY CLUB NAME - would use title: '{club_title}'")
                        if not club_title:
                            print(f"   ❌ NO TITLE EITHER - EXTRACTION WOULD FAIL!")
                            continue
                        else:
                            print(f"   ✅ Title available as fallback")
                    
                    club_info_found = True
                    
                    # Extract other data
                    squad_size = cells[2].get_text(strip=True) if len(cells) > 2 else "N/A"
                    avg_age = cells[3].get_text(strip=True) if len(cells) > 3 else "N/A"
                    foreigners = cells[4].get_text(strip=True) if len(cells) > 4 else "N/A"
                    
                    print(f"   📊 Squad size: {squad_size}")
                    print(f"   📊 Average age: {avg_age}")
                    print(f"   📊 Foreigners: {foreigners}")
                    
                    break
                else:
                    print(f"   ❌ URL pattern mismatch: {CLUB_URL_PATTERN} not in {href}")
        
        if not club_info_found:
            print(f"   ❌ NO CLUB INFO FOUND in first 2 cells")
    
    print(f"\n{'='*50}")
    print("🎯 EXTENDED ANALYSIS COMPLETE!")

if __name__ == "__main__":
    debug_table_parsing()