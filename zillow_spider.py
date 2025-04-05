import json
from copy import deepcopy
from urllib.parse import urlencode
import scrapy
from datetime import datetime

class ZillowSpiderSpider(scrapy.Spider):
    name = "zillow_spider"

    baseurl = 'https://www.zillow.com'
    custom_settings = {'ROBOTSTXT_OBEY': False,

                       'FEED_URI': f'outputs/ZILLOW_USA_WA{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
                       'FEED_FORMAT': 'csv',
                       # 'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
                       'FEED_EXPORT_ENCODING': 'utf-8',
                       'RETRY_TIMES': 5,
                       'CONCURRENT_REQUESTS': 1,
                       'DOWNLOAD_DELAY': 0.8,
                       'REDIRECT_ENABLED': False,

                       }
    headers = {
        'authority': 'www.zillow.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'cookie': 'zguid=24|%24f8149c67-7bd0-4ef3-b108-746c10f6c33d; zgsession=1|9ed24d21-90ca-4d10-9b6b-ad5fa3c88292; _ga=GA1.2.154075965.1694759461; _gid=GA1.2.1889301566.1694759461; zjs_anonymous_id=%22f8149c67-7bd0-4ef3-b108-746c10f6c33d%22; zjs_user_id=null; zg_anonymous_id=%22c4fbd29a-7f2d-4c27-91a2-1a69411bd4dd%22; _gcl_au=1.1.754909016.1694759462; DoubleClickSession=true; pxcts=5d9b234f-5391-11ee-aa84-835a3b460182; _pxvid=5d9b1299-5391-11ee-aa84-7003240f5c52; __pdst=9fbfc546e82c4f359c8d2fff2426eb2a; _fbp=fb.1.1694759470945.1062494261; _pin_unauth=dWlkPVl6VTBZamhqT1RNdE56RTNaaTAwTXpFeUxXRXhaalF0T1dFek5HVTFaR0ZtTkRJMQ; _clck=vxzcfk|2|ff1|0|1353; FSsampler=1584303648; JSESSIONID=66583C1CC76E38BCA5120EF6222DD02D; tfpsi=a0e89d16-7ea6-431e-8021-ab6b11f0d0e2; x-amz-continuous-deployment-state=AYABeG6w1r5i+ErWkjxGTMHdxNoAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3MjU1NjcyMVRZRFY4RDcyVlpWAAEAAkNEABpDb29raWUAAACAAAAADMR5UYc0CDQaBT30+gAwwnRke3FpcApRDjWya7EwBinqa5%2FYg3pb+vIYpV%2Fb8goDaJGSwPyXYQKKsQeeNDftAgAAAAAMAAQAAAAAAAAAAAAAAAAAAEQ6KTKqLNEi8ZLTvb9RkQn%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAwI6fyTVaWPRscGWA5qkQsDutZOd7mhtjPz0h39ZOd7mhtjPz0h3w==; _gat=1; _pxff_cc=U2FtZVNpdGU9TGF4Ow==; _pxff_cfp=1; _pxff_bsco=1; search=6|1697360638511%7Crect%3D34.267076234205256%2C-117.19449876171876%2C33.663424603955676%2C-119.35331223828126%26rid%3D95984%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26z%3D1%26listPriceActive%3D1%26baths%3D3.0-%26beds%3D4-%26fs%3D0%26fr%3D1%26mmm%3D0%26rs%3D0%26ah%3D0%26singlestory%3D0%26housing-connector%3D0%26abo%3D0%26garage%3D0%26pool%3D0%26ac%3D0%26waterfront%3D0%26finished%3D0%26unfinished%3D0%26cityview%3D0%26mountainview%3D0%26parkview%3D0%26waterview%3D0%26hoadata%3D1%26zillow-owned%3D0%263dhome%3D0%26featuredMultiFamilyBuilding%3D0%26excludeNullAvailabilityDates%3D0%26commuteMode%3Ddriving%26commuteTimeOfDay%3Dnow%09%0995984%09%7B%22isList%22%3Atrue%2C%22isMap%22%3Atrue%7D%09%09%09%09%09; __gads=ID=c7a3a86198dda95d:T=1694760246:RT=1694768640:S=ALNI_MbVemHp3Ohsvz2EOOm163M6CPFcNg; __gpi=UID=00000ca15bd90df1:T=1694760246:RT=1694768640:S=ALNI_MYSvBqEG0uJfMHB2PTjKdrOqrZF6Q; _hp2_id.1215457233=%7B%22userId%22%3A%227366386339100261%22%2C%22pageviewId%22%3A%222265972612853530%22%2C%22sessionId%22%3A%227803696774092854%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _px3=2070b6e5cf44b5c3affe88030ba6d71a5626f4f66e92a47212c37831ac84f08e:VaylFXsdK6FdXrKuaAG5oF8KYKs6ybhkKcAjJuC2bnYg8uF3jamc2JUI9cKU8vj5avgQ9EMw2jH27/zm/xSmLA==:1000:ZnJ3quocoDDMXVyXi499AoQiB5grv/Q9JgqP2WcYfHICpvzbzMG3kOSAkAWAqVGyhAnnL0t//fkZNmJISICy6UOb4ePunSKCVcPpKyROV/v3n9MSnGdz7vZUGQoZbcUJ2XAGzNr/f6zsVoKBcvoyE2hdgtSEC1nnV8Frqmbw/oUZX1gsKkd6kHgzdLD0EqOTQ3LL4A8jgMjKiLKj4gpj7g==; _hp2_ses_props.1215457233=%7B%22ts%22%3A1694768694517%2C%22d%22%3A%22www.zillow.com%22%2C%22h%22%3A%22%2F%22%7D; _uetsid=75b93b10539111eebca64148bd6f79ac; _uetvid=75b9ace0539111ee8d9e7bef1d45bf8f; _clsk=1rut27l|1694768696567|14|0|h.clarity.ms/collect; AWSALB=ePkECIdVK8bU7e9sv8Hb43Y4EZ5nKK71FLrf/BQp4sJHR7QodV+F9wJXra/JQHAhFjuMWVxqakdopppVOA+sSQOPMQWuYpnwiKuYWvUFl0JCbEytRsl6zR8cOXlE; AWSALBCORS=ePkECIdVK8bU7e9sv8Hb43Y4EZ5nKK71FLrf/BQp4sJHR7QodV+F9wJXra/JQHAhFjuMWVxqakdopppVOA+sSQOPMQWuYpnwiKuYWvUFl0JCbEytRsl6zR8cOXlE',
        'dnt': '1',
        'if-none-match': '"3c69d-z5hA0VpniYIUvZTn6KirNjliE8s"',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    }


    search_data = {
        'searchQueryState': '{"pagination":{"currentPage":1},"isMapVisible":true,"mapBounds":{"west":-95.84732113281251,"east":-95.00137386718751,"south":29.659612746641045,"north":29.975355921625816},"usersSearchTerm":"Houston, TX","regionSelection":[{"regionId":39051,"regionType":6}],"filterState":{"sort":{"value":"globalrelevanceex"},"ah":{"value":true}},"isListVisible":true}',
    }

    def start_requests(self):
        current_page = 1
        final_url=  f'https://www.zillow.com/king-county-wa/2_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A{current_page}%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-122.649335765625%2C%22east%22%3A-120.957441234375%2C%22south%22%3A46.96139087091063%2C%22north%22%3A47.901496025629314%7D%2C%22usersSearchTerm%22%3A%22King%20County%20WA%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A207%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22min%22%3A400000%2C%22max%22%3A750000%7D%2C%22mp%22%3A%7B%22min%22%3A2007%2C%22max%22%3A3762%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A9%7D'
        pass
        yield scrapy.Request(url=final_url, headers=self.headers, meta={'current_page': current_page,
                                                                        'final_url':final_url})
    def parse(self, response, **kwargs):
        current_page = response.meta.get('current_page','')
        final_url = response.meta.get('final_url','')

        a=response.css("#__NEXT_DATA__::text").get('')
        json_data = json.loads(response.css("#__NEXT_DATA__::text").get(''))
        homes_listing = json_data.get('props', {}).get('pageProps', {}).get('searchPageState', {}).get('cat1', {}).get(
            'searchResults', {}).get('listResults', [])

        for home in homes_listing:
            item = dict()
            item['address'] = home.get('address', '')
            item['addressCity'] = home.get('addressCity', '')
            item['addressState'] = home.get('addressState', '')
            item['addressStreet'] = home.get('addressStreet', '')
            item['addressZipcode'] = home.get('addressZipcode', '')
            item['zpid'] = home.get('zpid', '')
            item['area'] = home.get('area', '')
            item['baths'] = home.get('baths', '')
            item['beds'] = home.get('beds', '')
            item['imgURL'] = home.get('imgSrc', '')
            item['statusType'] = home.get('statusType', '')
            item['statusText'] = home.get('statusText', '')
            item['countryCurrency'] = home.get('countryCurrency', '')
            item['zestimate'] = home.get('hdpData', {}).get('homeInfo', {}).get('zestimate', '')
            item['rentZestimate'] = home.get('hdpData', {}).get('homeInfo', {}).get('rentZestimate', '')
            item['price'] = home.get('price', '')
            item['unformattedPrice'] = home.get('unformattedPrice', '')
            item['detailUrl'] = home.get('detailUrl', '')
            yield response.follow(url=item['detailUrl'], headers=self.headers, callback=self.detail_page,
                                  meta={'item': item})

        # current_page = response.meta['current_page']
        total_page = int(json_data.get('props', {}).get('pageProps', {}).get('searchPageState', {}).get('cat1', {}).
                         get('searchList', {}).get('totalPages', ''))

        if current_page<=total_page:
            current_page= current_page+1
            final_url=final_url.replace(f'pagination%22%3A%7B%22currentPage%22%3A{current_page-1}',f'pagination%22%3A%7B%22currentPage%22%3A{current_page}')
            print(final_url)
            yield scrapy.Request(url=final_url, headers=self.headers, callback=self.parse,
                                 meta={'current_page':current_page,'final_url':final_url})
    def detail_page(self, response):
        item = response.meta['item']
        json_data = json.loads(response.css("#__NEXT_DATA__::text").get(''))
        detail = json_data.get('props', {}).get('pageProps', {}).get('componentProps')
        home_detail = detail.get('gdpClientCache', '')
        pass
        with open('api.txt', 'w') as s:
            s.write(home_detail)
        if home_detail:
            home_data = json.loads(home_detail)
            detail_key = list(home_data.keys())[0]
            home = home_data.get(detail_key, {}).get('property', '')
            print("i am 2:::::::")
        else:
            home = detail.get('initialReduxState', {}).get('gdp', {}).get('building', {})
            print("i am 1:::::::")

        original_photos = []
        photos = home.get('originalPhotos', [])
        pass
        if not photos:
            photos = home.get('photos', [])
        for photo in photos:
            original_photos.append(photo.get('mixedSources', {}).get('jpeg', [])[-1].get('url', ''))
        item['original_photos'] = '\n'.join(original_photos)
        item['Facts'] = home.get('resoFacts', {}).get('atAGlanceFacts', [])
        item['propertySubType'] = home.get('resoFacts', {}).get('propertySubType', [])
        item['yearBuilt'] = home.get('yearBuilt', '')
        item['parking capacity'] = home.get('resoFacts', {}).get('parkingCapacity', '')
        item['price/sqft'] = home.get('resoFacts', {}).get('pricePerSquareFoot', '')
        item['Sold date'] = home.get('dateSoldString', '')
        # attributes info
        item['brokerageName'] = home.get('attributionInfo', {}).get('brokerName', '')
        item['Agennt Name'] = home.get('attributionInfo', {}).get('agentName', '')
        item['Agennt license Num'] = home.get('attributionInfo', {}).get('agentLicenseNumber', '')
        item['ListAOR'] = home.get('resoFacts', {}).get('listAOR', '')
        item['MLS Name'] = home.get('attributionInfo', {}).get('mlsName', '')
        item['MLS ID'] = home.get('attributionInfo', {}).get('mlsId', '')
        item['MLS Disclaimer'] = home.get('attributionInfo', {}).get('mlsDisclaimer', '')
        item['Last checked'] = home.get('attributionInfo', {}).get('lastChecked', '')
        item['Last updated'] = home.get('attributionInfo', {}).get('lastUpdated', '')
        # overview
        item['description'] = home.get('description', '')
        item['daysOnZillow'] = home.get('daysOnZillow', '')
        # facts and features
        item['lotSize'] = home.get('resoFacts', {}).get('lotSize', '')
        item['laundryFeatures'] = home.get('resoFacts', {}).get('laundryFeatures', [])
        item['lotFeatures'] = home.get('resoFacts', {}).get('lotFeatures', [])
        item['parkingFeatures'] = home.get('resoFacts', {}).get('parkingFeatures', [])
        item['poolFeatures'] = home.get('resoFacts', {}).get('poolFeatures', [])
        item['fireplaceFeatures'] = home.get('resoFacts', {}).get('fireplaceFeatures', [])
        item['communityFeatures'] = home.get('resoFacts', {}).get('communityFeatures', [])
        item['fireplaceFeatures'] = home.get('resoFacts', {}).get('fireplaceFeatures', [])
        item['accessibilityFeatures'] = home.get('resoFacts', {}).get('accessibilityFeatures', [])
        item['interiorFeatures'] = home.get('resoFacts', {}).get('interiorFeatures', [])
        item['spaFeatures'] = home.get('resoFacts', {}).get('spaFeatures', [])
        item['utilities'] = home.get('resoFacts', {}).get('utilities', [])
        item['lastSoldPrice'] = home.get('lastSoldPrice', '')
        item['cooling'] = home.get('resoFacts', {}).get('cooling', [])
        item['heating'] = home.get('resoFacts', {}).get('heating', [])
        item['sewer'] = home.get('resoFacts', {}).get('sewer', [])
        item['waterSource'] = home.get('resoFacts', {}).get('waterSource', [])
        item['spaFeatures'] = home.get('resoFacts', {}).get('spaFeatures', [])
        item['communityFeatures'] = home.get('resoFacts', {}).get('communityFeatures', [])
        item['foundationDetails'] = home.get('resoFacts', {}).get('foundationDetails', [])
        item['Flooring'] = home.get('resoFacts', {}).get('flooring', [])
        item['roofType'] = home.get('resoFacts', {}).get('roofType', '')
        item['level'] = home.get('resoFacts', {}).get('levels', '')
        item['stories'] = home.get('resoFacts', {}).get('stories', '')
        item['commonWalls'] = home.get('resoFacts', {}).get('commonWalls', '')
        item['entryLocation'] = home.get('resoFacts', {}).get('entryLocation', '')
        item['parcelNumber'] = home.get('resoFacts', {}).get('parcelNumber', '')
        item['zoning'] = home.get('resoFacts', {}).get('zoning', '')
        item['specialListingConditions'] = home.get('resoFacts', {}).get('specialListingConditions', '')
        item['hasAttachedProperty'] = home.get('resoFacts', {}).get('hasAttachedProperty', '')
        item['hasRentControl'] = home.get('resoFacts', {}).get('hasRentControl', '')
        item['isNewConstruction'] = home.get('resoFacts', {}).get('isNewConstruction', '')
        item['propertyCondition'] = home.get('resoFacts', {}).get('propertyCondition', '')
        item['cityRegion'] = home.get('resoFacts', {}).get('cityRegion', [])
        item['AgencyCompensation/%'] = home.get('resoFacts', {}).get('buyerAgencyCompensation', [])
        item['listingTerms'] = home.get('resoFacts', {}).get('listingTerms', [])
        item['schools'] = home.get('schools', [])
        item['nearbyHomes'] = home.get('nearbyHomes', [])
        item['nearbyCities'] = home.get('nearbyCities', [])
        item['nearbyNeighborhoods'] = home.get('nearbyNeighborhoods', [])
        item['nearbyzipcodes'] = home.get('nearbyZipcodes', [])
        item['comps'] = home.get('comps', [])
        item['priceHistory'] = home.get('priceHistory', [])
        item['taxHistory'] = home.get('taxHistory', [])
        yield item
