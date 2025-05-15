from playwright.sync_api import sync_playwright
import time
import pandas as pd
import os
from multiprocessing import Process

# url = "http://127.0.0.1:5000/get_level_nodeid"



# def compare_dataframes(df1, df2, comparison_columns):
#     try:
#         rows_in_merge_not_in_scraper = pd.merge(
#             df1, df2, how='left', on=comparison_columns, indicator=True
#         ).query("_merge == 'left_only'").drop(columns=["_merge"])
#         return rows_in_merge_not_in_scraper
#     except Exception as e:
        # return e

attributes = [
        {
            "li": "div.css-1dbjc4n.r-1l7z4oj.r-12kfsgm > ul > li",
            "name": "div.css-1dbjc4n.r-1l7z4oj.r-12kfsgm > ul > li > a > div",
            "link": "a.css-4rbku5.css-18t94o4.css-1dbjc4n.r-1loqt21.r-1otgn73.r-1i6wzkk.r-lrvibr"
        },
        {
            "li": "div.contentful.link-groupAlignStart",
            "name": "div.contentful.link-groupAlignStart > div > a > div > div",
            "link": "div.contentful.link-groupAlignStart > div > a"
        },
        {
            "li": "div.css-1dbjc4n>ul>li>div.css-1dbjc4n.r-16lk18l.r-11g3r6m",
            "name": "a.pulse-text-black.pulse-link.viz-nav-cta-text",
            "link": "a.pulse-text-black.pulse-link.viz-nav-cta-text"
        }
        # "identifier":[
        #     "._p13n-zg-nav-tree-all_style_zg-selected__1SfhQ",
        # ]
]

def scrape_chunk(chunk_data, level, idx):
    print(f"▶️ Starting scrape_chunk | Level: {level}, Chunk Index: {idx}")
    
    all_results = []
    not_found = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                java_script_enabled=True
            )

            try:
                for count, (_, page_info) in enumerate(chunk_data.iterrows(), 1):
                    try:
                        href = page_info.get("category_url", "")
                        if not isinstance(href, str) or not href.strip():
                            print(f"⚠️ Invalid link at row {count}")
                            not_found.append({
                                "category_url": str(href),
                                "category_name": page_info.get("category_name", ""),
                                "error": "Invalid or missing link"
                            })
                            continue

                        new_url = f"https://proxy.scrapeops.io/v1/?api_key=48d54767-b739-4f87-9296-3139ba494b02&url=https://www.cvs.com{href}" if "cvs.com" not in href else f"https://proxy.scrapeops.io/v1/?api_key=48d54767-b739-4f87-9296-3139ba494b02&url={href}"
                        print(f"🔗 Visiting: {new_url}")
                        parent_url = href

                        success = False
                        for attempt in range(3):
                            try:
                                page.goto(new_url, wait_until="domcontentloaded", timeout=20000)
                                time.sleep(7)
                                success = True
                                break
                            except Exception as e:
                                print(f"⚠️ Load error ({attempt+1}): {e}")
                                time.sleep(2)

                        if not success:
                            not_found.append({
                                "category_url": href,
                                "category_name": page_info.get("category_name", ""),
                                "error": "Page load failed"
                            })
                            continue

                        found_li_elements = False
                    
                        for layout in attributes:
                            li_selector = layout["li"]
                            name_selector = layout["name"]
                            link_selector = layout["link"]

                            try:
                                page.wait_for_selector(li_selector, timeout=30000)
                            except:
                                continue  # Skip if this layout doesn't exist

                            li_elements = page.query_selector_all(li_selector)
                            if not li_elements:
                                continue

                            found_li_elements = True

                            for li in li_elements:
                                page.wait_for_selector(name_selector, timeout = 5000)
                                page.wait_for_selector(link_selector, timeout = 5000)
                                name_el = li.query_selector(name_selector)
                                link_el = li.query_selector(link_selector)
                                data_type = None

                                if name_el and link_el:
                                    name = name_el.text_content().strip()
                                    link = link_el.get_attribute("href")
                                    data_type = "brand_data" if 'brand' in link else "category_data"
                                elif not link_el:
                                    # name = li.text_content().strip()
                                    link = li.get_attribute("href")
                                else:
                                    name = li.text_content().strip()
                                    link = li.get_attribute("href")

                                all_results.append({
                                "category_name": name,
                                "category_url": link,
                                "parent_category_url": parent_url,
                                "parent_category_name": page_info["category_name"],
                                "data_type": data_type,
                                "level": level
                                })

                            print(f"✅ Page {count}: Extracted with {li_selector}")

                        if not found_li_elements:
                            not_found.append({
                                "category_url": href,
                                "category_name": page_info.get("category_name", ""),
                                "error": "No LI elements found"
                            })
                            print(f"❌ Page {count} - No LI elements found")

                    except Exception as e:
                        print(f"❌ Error processing row {count}: {e}")
                        not_found.append({
                            "category_url": page_info.get("link", ""),
                            "category_name": page_info.get("category_name", ""),
                            "error": str(e)
                        })
                        continue

            finally:
                browser.close()
                if len(all_results) ==0 and len(not_found)==0:
                    p = Process(target=scrape_chunk, args=(chunk_data, level, idx))
                    p.start()

                df = pd.DataFrame(all_results)
                file_a = os.path.join(os.path.dirname(__file__), f'temp/level{level}_{idx}.csv')
                df.to_csv(file_a, index=False, encoding="utf-8-sig")
                print(f"📁 Saved results to {file_a}")

                df = pd.DataFrame(not_found)
                file_b = os.path.join(os.path.dirname(__file__), f'temp/level{level}_{idx}_not_found.csv')
                df.to_csv(file_b, index=False, encoding="utf-8-sig")
                print(f"📁 Saved not-found to {file_b}")

    except Exception as outer_e:
        print(f"🚨 Fatal error in scrape_chunk: {outer_e}")



def run():
        level = 0
        while True:
            file_x=os.path.join(os.path.dirname(__file__), f'main/level{level}.csv')
            data = pd.read_csv(file_x)
            chunk_size = 250
            level += 1
            if data.empty:
                print("No more data to process.")
                return
            idx=0
            all_chunked_files = []
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                f = os.path.join(os.path.dirname(__file__), f'temp/level{level}_{idx}.csv')
                all_chunked_files.append(f)
                p = Process(target=scrape_chunk, args=(chunk, level, idx))
                p.start()
                idx+=1
                if idx%6==0:
                    time.sleep(540)

            while True:
                missing_files = []
                for file_path in all_chunked_files:
                    if not os.path.exists(file_path):
                        missing_files.append(file_path)
                    else:
                        print(f"✅ Found: {file_path}")
                
                if len(missing_files)==0:
                    print('All files found')
                    break
                else:
                    print("⏳ File not found. Checking again in 1 minutes...")
                    time.sleep(60)
                
            if os.path.exists(os.path.join(os.path.dirname(__file__), 'temp')):
                all_files = os.listdir(os.path.join(os.path.dirname(__file__), 'temp'))
                actual_files = [f for f in all_files if f.startswith(f'level{level}')]
                found_data_files = [f for f in actual_files if not f.endswith('_not_found.csv')]
                not_found_data_files = [f for f in actual_files if f.endswith('_not_found.csv')]

                df_found = pd.DataFrame()
                df_not_found = pd.DataFrame()

                for i in found_data_files:
                    file_path = os.path.join(os.path.dirname(__file__), f'temp/{i}')
                    if os.path.getsize(file_path) > 0:  # ✅ Only read if file has data
                        try:
                            df = pd.read_csv(file_path)
                            if not df.empty:
                                df_found = pd.concat([df_found, df], ignore_index=True)
                        except pd.errors.EmptyDataError:
                            print(f"⚠️ Skipped corrupt or blank CSV: {file_path}")
                    else:
                        print(f"⚠️ File is empty, skipping: {file_path}")
                    # os.remove(file_path)

                df_found.to_csv(os.path.join(os.path.dirname(__file__), f'main/level{level}.csv'), index=False, encoding="utf-8-sig")

                for i in not_found_data_files:
                    file_path = os.path.join(os.path.dirname(__file__), f'temp/{i}')
                    if os.path.getsize(file_path) > 0:
                        try:
                            df = pd.read_csv(file_path)
                            if not df.empty:
                                df_not_found = pd.concat([df_not_found, df], ignore_index=True)
                        except pd.errors.EmptyDataError:
                            print(f"⚠️ Skipped corrupt or blank CSV: {file_path}")
                    else:
                        print(f"⚠️ File is empty, skipping: {file_path}")
                    # os.remove(file_path)

                df_not_found.to_csv(os.path.join(os.path.dirname(__file__), f'main/level{level}_not_found.csv'), index=False, encoding="utf-8-sig")

                # data = {
                #     "level": level
                # }

                # response = requests.post(url, json=data)
                # if response.status_code!=200:
                #     return
run()