from playwright.sync_api import sync_playwright
import time
import pandas as pd
from multiprocessing import Process
import os

attributes = {
        "li": [
            "[class*='apb-browse-refinements-indent-2']",
            "[class*='apb-browse-left-nav'] .left_nav ul a",
            "[class*='a-spacing-micro s-navigation-indent-2']",
        ],
        "name":[
            ".a-list-item>a>span",
            ".s-navigation-item>span",
            "a",
            ".a-color-base.a-link-normal>span"
        ],
        "links":[
            ".a-list-item>a",
            ".s-navigation-item",
            "a",
            ".a-color-base.a-link-normal",
        ],
        "identifier":[
            "._p13n-zg-nav-tree-all_style_zg-selected__1SfhQ",
        ]
    }

def scrape_chunk(data_chunk, level, chunk_index):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False to see the browser
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            }
        )
        page.goto(f"https://www.amazon.com")

        page.wait_for_selector("#nav-global-location-popover-link")
        page.click("#nav-global-location-popover-link")

        page.wait_for_selector("#GLUXZipUpdateInput")
        page.fill("#GLUXZipUpdateInput", "10001")

        page.wait_for_selector("#GLUXZipUpdate")
        page.click("#GLUXZipUpdate")

        page.locator("#GLUXConfirmClose").nth(1).click()
        level = 0
        while True:
            print(level)
            data = pd.read_csv(f'level{level}.csv')

            level += 1
            if data.empty:
                print("No more data to process.")
                break
            # df.to_csv('fourth1.0_new_found.csv', index=False)
            count=0
            all_results = []
            not_found = []


            MAX_RETRIES = 3
            RETRY_DELAY = 2
            for index, page_info in data.iterrows():
                try:
                    # if page_info["parent_category"] != "Electronics" and level==1:
                    #     continue
                    href = page_info["link"]
                    url = f"https://www.amazon.com{href}" if "amazon.com" not in href else href

                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            # print(f"⏳ Try {attempt} of {MAX_RETRIES}")
                            page.goto(url, wait_until="domcontentloaded")
                            # print("✅ Page loaded successfully")
                            break  # Exit retry loop if successful
                        except Exception as e:
                            print(f"⚠️ Error loading page on attempt {attempt}: {e}")
                            if attempt == MAX_RETRIES:
                                raise e  # Re-raise after final attempt
                            time.sleep(RETRY_DELAY)

                    # page.goto(url, wait_until="domcontentloaded")
                    found_li_elements = False
                    for li_selector in attributes['li']:
                        li_elements = page.query_selector_all(li_selector)
                        if not li_elements:
                            continue

                        found_li_elements = True
                        results = []
                        for li in li_elements:
                            name_el = None
                            link_el = None
                            for name_selector in attributes['name']:
                                name_el = li.query_selector(name_selector)
                                # print(name_el)
                                if name_el:
                                    break
                                
                            for link_selector in attributes['links']:
                                link_el = li.query_selector(link_selector)
                                if link_el:
                                    break

                            if name_el and link_el:
                                name = name_el.text_content().strip()
                                link = link_el.get_attribute("href")
                                results.append({
                                    "name": name,
                                    "link": link,
                                    "url": url,
                                    "parent_category": page_info["name"]
                                })
                            else:
                                name = li.text_content().strip()
                                link = li.get_attribute("href")
                                results.append({
                                    "name": name,
                                    "link": link,
                                    "url": url,
                                    "parent_category": page_info["name"]
                                })

                        if results:
                            print(f"\n✅ Page {count} - Using selectors:\nLI: {li_selector}\nNAME: {name_selector}\nLINK: {link_selector}")
                            all_results.extend(results)
                            break 
                        
                    count += 1
                    if not found_li_elements:
                        not_found.append({ 
                            "url": url,
                            "parent_category": page_info["name"],
                            "error": "No LI elements found",
                        })
                        print(f"❌ Page {count} - No LI elements found")
                        continue
                except Exception as e:
                    print(f"❌ Error on page {count} ({page_info.get('href')}): {e}")
                    continue
            

            df = pd.DataFrame(all_results)
            df.to_csv(f"main/level{level}_part{chunk_index}.csv", index=False)
            
            df = pd.DataFrame(not_found)
            df.to_csv(f"main/level{level}_part{chunk_index}_not_found.csv", index=False)
            time.sleep(2)

        browser.close()


def run_parallel(level):
    data = pd.read_csv(f"level{level}.csv")
    num_processes = 4
    chunk_size = len(data) // num_processes + 1
    processes = []

    for i in range(num_processes):
        chunk = data.iloc[i*chunk_size : (i+1)*chunk_size]
        p = Process(target=scrape_chunk, args=(chunk, level, i))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # Combine output
    result_dfs = []
    not_found_dfs = []
    for i in range(num_processes):
        result_path = f"level{level}_part{i}.csv"
        not_found_path = f"level{level}_part{i}_not_found.csv"
        if os.path.exists(result_path):
            result_dfs.append(pd.read_csv(result_path))
        if os.path.exists(not_found_path):
            not_found_dfs.append(pd.read_csv(not_found_path))
    
    pd.concat(result_dfs, ignore_index=True).to_csv(f"level{level+1}.csv", index=False)
    pd.concat(not_found_dfs, ignore_index=True).to_csv(f"level{level+1}_not_found.csv", index=False)


def run():
    level = 0
    while True:
        input_file = f"level{level}.csv"
        if not os.path.exists(input_file):
            print("No more levels.")
            break
        run_parallel(level)
        level += 1

        
if __name__ == "__main__":
    run()