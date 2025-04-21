from playwright.sync_api import sync_playwright
import json

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False to see the browser
        page = browser.new_page()
        page.goto("https://www.amazon.com/ref=nav_logo")

        page.wait_for_selector("#nav-global-location-popover-link")
        page.click("#nav-global-location-popover-link")

        page.wait_for_selector("#GLUXZipUpdateInput")
        page.fill("#GLUXZipUpdateInput", "10001")

        page.wait_for_selector("#GLUXZipUpdate")
        page.click("#GLUXZipUpdate")

        # page.wait_for_selector("input#GLUXConfirmClose")
        page.locator("#GLUXConfirmClose").nth(1).click()
        # page.wait_for_load_state("load")
        # page.click("[data-action='GLUXConfirmAction']")
        # page.wait_for_load_state("domcontentloaded")
        # Wait for content to load if needed
        page.wait_for_selector("#nav-hamburger-menu")
        page.click("#nav-hamburger-menu")

        page.wait_for_selector("[aria-labelledby='Shop by Department'] > ul > li > a")
        lis = page.query_selector_all("[aria-labelledby='Shop by Department'] > ul > li > a")

        page.wait_for_selector("[aria-labelledby='Shop by Department']>ul>li+ul>li>a")
        lis2 = page.query_selector_all("[aria-labelledby='Shop by Department']>ul>li+ul>li>a")
        
        parent_category_data = []
        seen_category = set()
        for li in lis:
            category_text = li.text_content().strip()
            if category_text not in seen_category:
                data_menu_id = li.get_attribute("data-menu-id")
                parent_category_data.append({
                    "category": category_text,
                    "data_menu_id": data_menu_id,
                    "sub_categories": []
                })
                seen_category.add(category_text)

        for li in lis2:
            category_text = li.text_content().strip()
            if category_text not in seen_category:
                data_menu_id = li.get_attribute("data-menu-id")
                parent_category_data.append({
                    "category": category_text,
                    "data_menu_id": data_menu_id,
                    "sub_categories": []
                })
                seen_category.add(category_text)


        filtered_list = [x for x in parent_category_data if x["category"] != "See all" or x["category"] != "See less"]


        # categories = set(x["category"] for x in filtered_list)

        # print(filtered_list)

        # page.wait_for_selector(".hmenu.hmenu-translateX-right")
        sub_category = page.query_selector_all(".hmenu.hmenu-translateX-right")

        sub_category_list = []
        for cat in sub_category:
            seen_sub_category = set()
            for x in filtered_list:
                if x['data_menu_id'] == cat.get_attribute("data-menu-id"):
                    list_in_cat = cat.query_selector_all(".category-section>ul>li>.hmenu-item")
                    for c in list_in_cat:
                        name = c.inner_html()
                        if name not in seen_sub_category:
                            link = c.get_attribute("href")
                            sub_category_list.append({
                                'parent_category':x['category'],
                                'sub_category_name':name,
                                'href':link
                            })
                            seen_sub_category.add(name)
                    # x["sub_categories"] = sub_category_list.copy()

        print(filtered_list)
        new_sub_category_list = [d for d in sub_category_list if d["parent_category"] not in ["See all", "See less"]]
        with open("filtered_data.json", "w", encoding="utf-8") as f:
            json.dump(new_sub_category_list, f, ensure_ascii=False, indent=4)

        browser.close()

        
if __name__ == "__main__":
    run()