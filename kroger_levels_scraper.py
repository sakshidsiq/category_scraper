from playwright.sync_api import sync_playwright
import json
import time
import pandas as pd
from celery import Celery
import os

WEBSHARE_PROXY = {
    "server": f"http://{os.getenv("WEBSHARE_USERNAME")}-rotate:{os.getenv("WEBSHARE_PASSWORD")}@p.webshare.io:80",
}
app = Celery('levels_scraper',
             broker='redis://:JustWin12@localhost:6379/0',
             backend='redis://:JustWin12@localhost:6379/0') 

print(WEBSHARE_PROXY)
attributes = {
        "li":[
            ".ImageLink-wrapper.block.mx-auto.text-center",
        ],
        "name":[
            ".kds-Link.kds-Link--l.kds-Link--implied.ImageLink-subText.p-6.text-primary",
        ],
        "links":[
            ".kds-Link.kds-Link--l.kds-Link--implied.ImageLink-subText.p-6.text-primary",
        ],
        "viewall":[
            ".ImageNav-itemWrapper.ImageNav-viewAll.text-center>button",
        ]
    }

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'device-memory': '8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
}

def concatenate_csvs(level: int, directory: str = "./levels/level", output_filename_found: str = None, output_filename_not_found: str = None):
    path = f"{directory}{level}/"
    files = os.listdir(path)

    found_files = [
        os.path.join(path, f)
        for f in files
        if f.startswith(f"level{level}_part") and f.endswith(".csv") and "_not_found" not in f
    ]

    not_found_files = [
        os.path.join(path, f)
        for f in files
        if f.startswith(f"level{level}_part") and f.endswith("_not_found.csv")
    ]

    def extract_part_number(filename):
        return int(filename.split("part")[1].split("_")[0].split(".")[0])

    found_files.sort(key=extract_part_number)
    not_found_files.sort(key=extract_part_number)

    def load_csvs(file_list, label):
        dfs = []
        for file in file_list:
            print(f"Loading {label}: {file}")
            try:
                df = pd.read_csv(file)
                if df.empty and df.columns.size == 0:
                    print(f"⚠️ Skipping empty {label} file: {file}")
                    continue
                dfs.append(df)
            except pd.errors.EmptyDataError:
                print(f"⚠️ Skipping completely empty {label} file: {file}")
                continue
        return dfs

    found_dfs = load_csvs(found_files, "found")
    not_found_dfs = load_csvs(not_found_files, "not_found")
    save_path = os.path.join(f"./kroger_menu_levels/")

    # Save found
    if found_dfs:
        combined_found = pd.concat(found_dfs, ignore_index=True)
        if output_filename_found is None:
            output_filename_found = os.path.join(save_path, f"level{level}.csv")
            all_level_file = os.path.join(save_path, f"all_level.csv")

        if os.path.exists(output_filename_found):
            print(f"📄 {output_filename_found} already exists. Appending data.")
            existing_df = pd.read_csv(output_filename_found)
            combined_found = pd.concat([existing_df, combined_found], ignore_index=True)

        combined_found = combined_found.drop_duplicates()
        combined_found.to_csv(output_filename_found, index=False)
        print(f"✅ Found file written to: {output_filename_found}")

        all_levels = pd.read_csv(all_level_file)
        this_level = pd.read_csv(output_filename_found)

        column_to_ignore = 'level'
        compare_cols = [col for col in all_levels.columns if col != column_to_ignore]
        existing_rows = set(all_levels[compare_cols].itertuples(index=False, name=None))
        data2_tuples = list(this_level[compare_cols].itertuples(index=False, name=None))
        mask = [row not in existing_rows for row in data2_tuples]
        df2_filtered = this_level[mask]
        combined = pd.concat([all_levels, df2_filtered], ignore_index=True)
        combined.to_csv("./kroger_menu_levels/all_levels.csv", index=False)

    else:
        print("❌ No valid found data to write.")

    # Save not found
    if not_found_dfs:
        combined_not_found = pd.concat(not_found_dfs, ignore_index=True)
        if output_filename_not_found is None:
            output_filename_not_found = os.path.join(save_path, f"level{level}_not_found.csv")
        
        if os.path.exists(output_filename_not_found):
            print(f"📄 {output_filename_not_found} already exists. Appending data.")
            existing_df = pd.read_csv(output_filename_not_found)
            combined_not_found = pd.concat([existing_df, combined_not_found], ignore_index=True)
        
        combined_not_found = combined_not_found.drop_duplicates()
        combined_not_found.to_csv(output_filename_not_found, index=False)

        print(f"✅ Not found file written to: {output_filename_not_found}")
    else:
        print("❌ No valid not found data to write.")

@app.task(name='scraper_task', queue='my_scraper_queue')
def scraper_celery_task(part, chunk, level):
    chunk = pd.DataFrame(chunk)
    with sync_playwright() as p:
        result = []
        not_found = []
        for index, row in chunk.iterrows():
            # if row['parent_category'] not in ['Candy','Vitamins & Supplements']:
            #     continue
            url = row['category_link']
            reason = None
            if pd.notna(url) and isinstance(url, str) :
                if "kroger.com" not in url:
                    url = f"https://www.kroger.com{url}"

                try:
                    for tries in range(5):
                        try:
                            browser = p.chromium.launch(headless=False)
                            context = browser.new_context(
                                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                                viewport={"width": 1280, "height": 800},
                                java_script_enabled=True,
                                proxy={
                                    "server": f"http://<username>-rotate:<password>@p.webshare.io:80",
                                    "username": "ketankedar-rotate",
                                    "password": "JustWin123",
                                },
                                extra_http_headers={
                                    "Authorization": "Token edblymim618i2q2mtc6f70p9xcss8jqkcm9qucek"
                                }
                            )
                            page = context.new_page()
                            page.goto(url, wait_until="domcontentloaded")
                            reason = None
                            break
                        except Exception as e:
                            reason = "page not loaded"
                            print(f"{index}) Didn't load page: {url} due to: {e}")
                            print(f"{index}) Retrying... {tries+1}/5")
                        finally:
                            try:
                                browser.close()
                            except:
                                pass
                                            

                        
                    for viewbtn in attributes['viewall']:
                        try:
                            page.wait_for_selector(viewbtn, timeout=2000)
                            view_all = page.query_selector(viewbtn)
                            view_all.click()
                        except:
                            print(f"{index}) Selector not found: {viewbtn}")
                            continue
                    for li in attributes['li']:
                        try:
                            page.wait_for_selector(li, timeout=2000)
                            blocks = page.query_selector_all(li)

                            if not blocks:  
                                print(f"{index}) No blocks found for selector: {li}")
                                continue

                            for block in blocks:
                                name = None
                                link = None
                                data_type = None

                                for attr in attributes['name']:
                                    try:
                                        page.wait_for_selector(attr,timeout=2000)
                                        name_el = block.query_selector(attr)
                                        name = name_el.inner_text()
                                    except:
                                        print(f"{index}) Selector not found: {attr}")
                                        continue

                                for attr in attributes['links']:
                                    try:
                                        page.wait_for_selector(attr,timeout=2000)
                                        link_el = block.query_selector(attr)
                                        link = link_el.get_attribute("href")
                                        if "brandName" in link:
                                            data_type = "brand"
                                        else:
                                            data_type = "category"
                                    except:
                                        print(f"{index}) Selector not found: {attr}")
                                        continue

                                result.append({
                                    "parent_category": row['category'],
                                    "parent_category_link": row['category_link'],
                                    "data_type": data_type,
                                    "category": name,
                                    "category_link": link,
                                    "level": level,
                                })
                        except:
                            print(f"{index}) Selector not found: {li}")
                            if reason:
                                not_found.append({
                                    "parent_category": row['category'],
                                    "category_link": row['category_link'],
                                    "reason": reason
                                })
                            else:
                                not_found.append({
                                    "parent_category": row['category'],
                                    "category_link": row['category_link'],
                                    "reason": f"Selector not found: {li}"
                                })

                            continue
                except Exception as e:
                    # page.goto( f"http://{os.getenv("WEBSHARE_USERNAME")}-rotate:{os.getenv("WEBSHARE_PASSWORD")}@p.webshare.io:80?url={url}",timeout=5000)
                    print(f"{index}) Error navigating to {url}: {e}")
                    continue
                finally:
                    browser.close()

        res = pd.DataFrame(result)
        notfound = pd.DataFrame(not_found)
        output_dir = f"levels/level{level}"
        os.makedirs(output_dir, exist_ok=True)

        if 'data_type' in res.columns:
            category_df = res[res['data_type'] == 'category'].reset_index(drop=True).drop(columns=["data_type"])
            brand_df = res[res['data_type'] == 'brand'].reset_index(drop=True).drop(columns=["data_type"])

            category_df.to_csv(f"{output_dir}/level{level}_part{part}.csv", index=False)
            brand_df.to_csv(f"{output_dir}/brand{level}_part{part}.csv", index=False)
        else:
            print(f"⚠️ No 'data_type' column found in result for part {part}, level {level}")
            res.to_csv(f"{output_dir}/level{level}_part{part}.csv", index=False)

        notfound.to_csv(f"{output_dir}/level{level}_part{part}_not_found.csv", index=False)

def run():
    level = 6
    while True:
        data = pd.read_csv(f"kroger_menu_levels/level{level}.csv")
        chunk_size = 70 
        level += 1
        
        if data.empty:
            print(f"Level {level} data is empty. Exiting.")
            return 
        
        if level == 10:
            break

        chunk_dfs = []
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            chunk_dfs.append(chunk)
        max_concurrent_tasks = 6
        active_tasks = []

        for part, chunk in enumerate(chunk_dfs):
            active_tasks = [t for t in active_tasks if not t.ready()]

            while len(active_tasks) >= max_concurrent_tasks:
                time.sleep(1)
                active_tasks = [t for t in active_tasks if not t.ready()]

            print("Submitting new task...")
            result = scraper_celery_task.delay(part, chunk.to_dict(orient="records"), level)
            active_tasks.append(result)
                
        print("✅ All tasks submitted. Waiting for them to complete...")
        while any(not task.ready() for task in active_tasks):
            time.sleep(2)

        concatenate_csvs(level=level)
            
        
if __name__ == "__main__":
    run()