    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        """
        Processes a single offer item by crawling its detailed page and extracting information.

        Args:
            item (Any): A dictionary containing 'link', 'title', and 'main_page_link' for the offer.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted data and output path if successful, else None.
        """
        offer_name = item['title']
        main_page_url = item['main_page_link']
        programa_php_url = item['link']

        # Generate a slug for the offer name to use in the output filename.
        offer_slug = slugify(offer_name)
        output_path = os.path.join(self.output_dir, f"{offer_slug}.json")

        print(f"Processing offer: {offer_name}")
        print(f"Main Page URL: {main_page_url}")
        print(f"Programa.php URL: {programa_php_url}")

        detailed_page_html = await self._get_detailed_page_html(main_page_url, programa_php_url)

        if not detailed_page_html:
            print(f"Failed to get detailed page HTML for {offer_name}")
            return None

        # Save the detailed page HTML for debugging
        with open("/home/dani/Desktop/Crawl4AI/deepseek-crawler/debug_detailed_page.html", "w", encoding="utf-8") as f:
            f.write(detailed_page_html)

        detailed_offer_data = await self._parse_detailed_offer_content(detailed_page_html, offer_name, main_page_url)
        if detailed_offer_data:
            return {"data": detailed_offer_data.model_dump(), "path": output_path}
        else:
            print(f"No detailed data extracted or incomplete for {main_page_url}")
        
        return None

    async def _get_detailed_page_html(self, main_page_url: str, programa_php_url: str) -> Optional[str]:
        """
        Navigates to the main page, clicks on an offer link, and returns the detailed page HTML.
        """
        try:
            # Use the existing crawler instance to navigate
            main_page_config = CrawlerRunConfig(
                url=main_page_url,
                verbose=True,
                # No js_code here, we'll use Playwright directly
            )
            main_page_result = await self.crawler.arun(main_page_url, config=main_page_config)

            if not main_page_result or not main_page_result.page:
                print(f"Failed to load main page: {main_page_url}")
                return None

            page = main_page_result.page

            # Find the link that matches the programa_php_url
            # This assumes the link is present on the main page or within its iframe
            # You might need to adjust the selector based on actual HTML structure
            link_selector = f'a[href*="{os.path.basename(programa_php_url)}"]'
            offer_link_element = await page.locator(link_selector).first

            if not offer_link_element:
                print(f"Could not find offer link for {programa_php_url} on {main_page_url}")
                return None

            # Click the link and wait for navigation
            await offer_link_element.click()
            await page.wait_for_load_state('networkidle')

            # Return the content of the detailed page
            return await page.content()

        except Exception as e:
            print(f"Error in _get_detailed_page_html: {e}")
            return None

    async def _parse_detailed_offer_content(self, html_content: str, offer_name: str, detailed_offer_link: Optional[str]) -> Optional[AngelTravelDetailedOffer]:
        """
        Parses the HTML content of a detailed offer page to extract specific information.

        Args:
            html_content (str): The HTML content of the page.
            offer_name (str): The name of the offer.
            detailed_offer_link (Optional[str]): The URL of the detailed offer page.

        Returns:
            Optional[AngelTravelDetailedOffer]: An instance of AngelTravelDetailedOffer with extracted data, or None if parsing fails.
        """
        # Initialize BeautifulSoup to parse the HTML content.
        soup = BeautifulSoup(html_content, 'html.parser')

        program = ""
        included_services = []
        excluded_services = []

        # Attempt to extract the program details using a predefined CSS selector.
        program_element = soup.select_one(CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_PROGRAM)
        print(f"DEBUG: program_element: {program_element}")
        if program_element:
            # Extract text, preserving newlines for better readability and stripping extra whitespace.
            program = program_element.get_text(separator='\n', strip=True)
            print(f"DEBUG: Extracted program: {program}")

        # Find the 'Цената включва:' heading
        included_heading = soup.find('h3', string='Цената включва:')
        if included_heading:
            current_element = included_heading.find_next_sibling()
            while current_element and current_element.name not in ['h3']:
                print(f"DEBUG: Included current_element: {current_element.name}")
                if current_element.name == 'ul':
                    included_services.extend([li.get_text(strip=True) for li in current_element.find_all('li') if li.get_text(strip=True)])
                elif current_element.name == 'p':
                    text = current_element.get_text(strip=True)
                    if text:
                        included_services.append(text)
                current_element = current_element.find_next_sibling()
            print(f"DEBUG: Extracted included_services: {included_services}")

        # Find the 'Цената не включва:' heading
        excluded_heading = soup.find('h3', string='Цената не включва:')
        if excluded_heading:
            current_element = excluded_heading.find_next_sibling()
            while current_element and current_element.name not in ['h3']:
                print(f"DEBUG: Excluded current_element: {current_element.name}")
                if current_element.name == 'ul':
                    excluded_services.extend([item.strip() for item in current_element.get_text(separator='\n', strip=True).split('\n') if item.strip()])
                elif current_element.name == 'p':
                    text = current_element.get_text(strip=True)
                    if text:
                        excluded_services.append(text)
                current_element = current_element.find_next_sibling()
            print(f"DEBUG: Extracted excluded_services: {excluded_services}")

        # If an offer name is provided, create and return an AngelTravelDetailedOffer object.
        if offer_name:
            detailed_offer = AngelTravelDetailedOffer(
                offer_name=offer_name,
                program=program,
                included_services=included_services,
                excluded_services=excluded_services,
                detailed_offer_link=detailed_offer_link
            )
            return detailed_offer
        return None
