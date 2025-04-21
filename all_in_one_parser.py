from playwright.sync_api import sync_playwright
import json


attributes = {
        "li": [
            "[class*='apb-browse-refinements-indent-2']",
            "[class*='apb-browse-left-nav'] .left_nav ul",
            "[class*='a-spacing-micro s-navigation-indent-2']"
        ],
        "name":[
            "[class*='apb-browse-refinements-indent-2']>.a-list-item>a>span",
            "[class*='a-spacing-micro s-navigation-indent-2'] .s-navigation-item>span",
            "[class*='apb-browse-refinements-indent-2'] .a-color-base.a-link-normal>span"
        ],
        "links":[
            "[class*='apb-browse-refinements-indent-2']>.a-list-item>a",
            "[class*='a-spacing-micro s-navigation-indent-2'] .s-navigation-item",
            "[class*='apb-browse-refinements-indent-2'] .a-color-base.a-link-normal",
        ]
    }

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False to see the browser
        page = browser.new_page()

        with open("filtered_data.json", "r") as file:
            data = json.load(file)
        count=0
        all_results = []
        page.goto(f"https://www.amazon.com")

        page.wait_for_selector("#nav-global-location-popover-link")
        page.click("#nav-global-location-popover-link")

        page.wait_for_selector("#GLUXZipUpdateInput")
        page.fill("#GLUXZipUpdateInput", "10001")

        page.wait_for_selector("#GLUXZipUpdate")
        page.click("#GLUXZipUpdate")

        page.locator("#GLUXConfirmClose").nth(1).click()

        for page_info in data:
            try:
                if page_info["parent_category"] != "Electronics":
                    continue

                href = page_info["href"]
                url = f"https://www.amazon.com{href}" if "amazon.com" not in href else href
                page.goto(url, wait_until="domcontentloaded")

                for li_selector in attributes['li']:
                    li_elements = page.query_selector_all(li_selector)
                    if not li_elements:
                        continue

                    for name_selector, link_selector in zip(attributes['name'], attributes['links']):
                        results = []

                        for li in li_elements:
                            name_el = li.query_selector(name_selector)
                            link_el = li.query_selector(link_selector)

                            if name_el and link_el:
                                name = name_el.text_content().strip()
                                link = link_el.get_attribute("href")
                                if name and link:
                                    results.append({"name": name, "link": link})

                        if results:
                            print(f"\n✅ Page {count} - Using selectors:\nLI: {li_selector}\nNAME: {name_selector}\nLINK: {link_selector}")
                            all_results.extend(results)
                            break  # break out of name/link loop

                    if results:
                        break  # break out of li_selector loop

            except Exception as e:
                print(f"❌ Error on page {count} ({page_info.get('href')}): {e}")
                continue
        
        with open(f"first.json", "w") as first:
            json.dump(results, first, ensure_ascii=False, indent=4)

        browser.close()

        
if __name__ == "__main__":
    run()