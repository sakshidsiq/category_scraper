from playwright.sync_api import sync_playwright
import json
import time
import pandas as pd
import os

PROXY = {
    "server": f"http://scraperapi.country=us:{os.getenv("SCRAPER_API_KEY")}@proxy-server.scraperapi.com:8001",
    }


attributes = {
        "ul": [
            ".SecondCategoryDisplayer",
        ],
        "li":[
            ".ContentDisplayer>.ContentBlock>li",
        ],
        "name":[
            "button",
            "a",
        ],
        "links":[
            "a",
        ],
        "heading":[
            "h2",
        ]
    }

def run():
    with sync_playwright() as p:
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
            'cookie': 'pid=8f9cd50a-b84f-3aec-12ed-0544a1f1b744; sid=17a2b91a-777e-3fd9-0ef2-e56c236964e7; origin=fldc',
        }
        browser = p.chromium.launch(headless=False) 
        page = browser.new_page(
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
        page.set_extra_http_headers(headers)
        try:
            page.goto(f"https://www.kroger.com/")
        except Exception as e:
            print(f"Error navigating to Kroger: {e}")
            page.goto(f"https://www.kroger.com/")

        data = []
        level0 =[]
        level1 =[]
        level2 =[]
        level3 =[]
        level4 =[]
        def checker(nav_category):
            for ul in attributes['ul']:
                try:
                    page.wait_for_selector(ul, timeout=3000)
                    button_blocks = page.query_selector_all(ul)
                    return len(button_blocks)-1
                except:
                    print(f"Selector not found: {ul}")
                    return 101
                    continue 

        def button_blocks_container(level,nav_category, main_cat=None):
            for ul in attributes['ul']:
                try:
                    page.wait_for_selector(ul, timeout=3000)
                    button_blocks = page.query_selector_all(ul)
                    clicker(button_blocks[level], main_cat,nav_category)
                except:
                    print(f"Selector not foundfsrg: {ul}")
                    clicker(None, main_cat,nav_category)
                    continue 
        
        def clicker(button_blocks, main_cat, nav_category):
            if button_blocks is None:
                print("Button blocks not found")
                category_scrape(None, main_cat, nav_category)
                return
            li_tags = button_blocks.query_selector_all('li')
            for btn in range(0,len(li_tags)):
                main_category = None
                main_cat_name = None
                for name_selector in attributes['name']:
                    main_category = li_tags[btn].query_selector(name_selector)
                    if main_category:
                        if btn > 0:
                            main_category.click()
                        count = 0
                        if not main_cat:
                            count = checker(nav_category)
                        main_cat_name = main_category.text_content().strip()
                        if count == 0:
                            category_scrape(main_cat_name, main_cat, nav_category)
                        elif count == 101:
                            category_scrape(main_cat_name, main_cat, nav_category)
                        else:
                            button_blocks_container(level=count, main_cat=main_cat_name, nav_category=nav_category)
                        break
                    else: continue

        def category_scrape(parent_cat, main_cat, nav_category):
            for li in attributes['li']:
                try:
                    page.wait_for_selector(li)
                    content_nav = page.query_selector_all(li)
                except:
                    print(f"Selector not found: {li}") 
                    continue

            heading = None
            results = []
            for category in content_nav:
                for heading_selector in attributes['heading']:
                    try:
                        heading_tag = category.query_selector(heading_selector)
                        if heading_tag:
                            heading = heading_tag.text_content().strip()
                            break
                    except:
                        continue
                name = None
                link = None
                for name_selector in attributes['name']:
                    try:
                        name_tag = category.query_selector(name_selector)
                        if name_tag:
                            name = name_tag.text_content().strip()
                            break
                    except:
                        continue

                for link_selector in attributes['links']:
                    try:
                        link_tag = category.query_selector(link_selector)
                        if link_tag:
                            link = link_tag.get_attribute("href")
                            break
                    except:
                        continue

                if main_cat:
                    level0.append({
                        "parent_category": None,
                        "parent_category_link": None,
                        "category": nav_category,
                        "category_link": None,
                        "level": 0,
                    })
                    level1.append({
                        "parent_category": nav_category,
                        "parent_category_link": None,
                        "category": main_cat,
                        "category_link": None,
                        "level": 1,
                    })
                    level2.append({
                        "parent_category": main_cat,
                        "parent_category_link": None,
                        "category": parent_cat,
                        "category_link": None,
                        "level": 2,
                    })
                    level3.append({
                        "parent_category": parent_cat,
                        "parent_category_link": None,
                        "category": heading,
                        "category_link": None,
                        "level": 3,
                    })
                    level4.append({
                        "parent_category": heading,
                        "parent_category_link": None,
                        "category": name,
                        "category_link": link,
                        "level": 4,
                    })
                elif not main_cat and not parent_cat:
                    # continue
                    level0.append({
                        "parent_category": None,
                        "parent_category_link": None,
                        "category": nav_category,
                        "category_link": None,
                        "level": 0,
                    })
                    level1.append({
                        "parent_category": nav_category,
                        "parent_category_link": None,
                        "category": heading,
                        "category_link": None,
                        "level": 1,
                    })
                    level2.append({
                        "parent_category": heading,
                        "parent_category_link": None,
                        "category": name,
                        "category_link": link,
                        "level": 2,
                    })
                else:
                    level0.append({
                        "parent_category": None,
                        "parent_category_link": None,
                        "category": nav_category,
                        "category_link": None,
                        "level": 0,
                    })
                    level1.append({
                        "parent_category": nav_category,
                        "parent_category_link": None,
                        "category": parent_cat,
                        "category_link": None,
                        "level": 1,
                    })
                    level2.append({
                        "parent_category": parent_cat,
                        "parent_category_link": None,
                        "category": heading,
                        "category_link": None,
                        "level": 2,
                    })
                    level3.append({
                        "parent_category": heading,
                        "parent_category_link": None,
                        "category": name,
                        "category_link": link,
                        "level": 3,
                    })
            return results
        
        page.wait_for_selector(".ExposedMenu-TopNavV2")
        top_nav = page.query_selector_all(".ExposedMenu-TopNavV2")
        for nav in top_nav:
            try:
                nav_category = nav.inner_text()
                nav.click()
                button_blocks_container(level=0, nav_category=nav_category)
            except Exception as e:
                print(f"Error clicking nav: {e}")
                continue
        page.click("#ExposedMenu-Category-Shop")
        
        level0df = pd.DataFrame(level0)
        level1df = pd.DataFrame(level1)
        level2df = pd.DataFrame(level2)
        level3df = pd.DataFrame(level3)
        level4df = pd.DataFrame(level4)

        level0df = level0df.drop_duplicates()

        level1df = level1df.drop_duplicates(subset=['parent_category','category'])
        level2df = level2df.drop_duplicates(subset=['parent_category','category'])
        level3df = level3df.drop_duplicates(subset=['parent_category','category'])
        level4df = level4df.drop_duplicates(subset=['parent_category','category'])


        # df = pd.DataFrame(data)
        # df = df.drop_duplicates()
        # df = df.drop_duplicates(subset=['parent_category','category'])
        level0df.to_csv("kroger_menu_levels/level0.csv", index=False)
        level1df.to_csv("kroger_menu_levels/level1.csv", index=False)
        level2df.to_csv("kroger_menu_levels/level2.csv", index=False)
        level3df.to_csv("kroger_menu_levels/level3.csv", index=False)
        level4df.to_csv("kroger_menu_levels/level4.csv", index=False)

        browser.close()

        
if __name__ == "__main__":
    run()