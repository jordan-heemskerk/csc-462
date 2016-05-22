#!/usr/bin/python

from bs4 import BeautifulSoup

class FacebookMessageParser(object):

    def parser(self):
        # indexing constants
        WEEKDAY = 0
        MONTH = 1
        YEAR = 2

        soup = BeautifulSoup(open("messages.htm"), "lxml")

        user_title = soup.title.string
        user_name = user_title.split(" -")
        user_name = user_name[0]

        headers = soup.findAll('div', attrs={'class':'message_header'})
        for header in headers:

            meta_user_text = header.find('span', attrs={'class':'user'}).get_text()
            meta_date_text = header.find('span', attrs={'class':'meta'}).get_text()

            # e.g., [u'Sunday', u' April 5', u' 2015 at 8:44am PDT']
            meta_date_text = [item.strip() for item in meta_date_text.split(",")]

            meta_weekday = meta_date_text[WEEKDAY]

            # split month, date
            meta_date_text[MONTH]   = meta_date_text[MONTH].split(" ")
            meta_month              = meta_date_text[MONTH][0]
            meta_date               = meta_date_text[MONTH][1]

            # split year
            meta_date_text[YEAR]    = meta_date_text[YEAR].split(" ")
            meta_year               = meta_date_text[YEAR][0]

            # split time
            raw_time = meta_date_text[YEAR][2]
            if "pm" in raw_time:
                raw_time = raw_time.split(":")
                hour = int(raw_time[0]) + 12

                if hour >= 24:
                    hour = 00

                raw_time[0] = str(hour)

                raw_time = ":".join(raw_time)

            # save time without am/pm suffix
            meta_time = raw_time[:-2]

            try:
                # don't print messages sent to self or old [deleted] users
                if user_name != meta_user_text and "@facebook" not in meta_user_text:
                    # print data to file e.g., Billy Mays;Sunday,April,5,2015,8:44
                    print '%s;%s,%s,%s,%s,%s' % (meta_user_text, meta_weekday, meta_month, meta_date, meta_year, meta_time)
            except (UnicodeDecodeError, UnicodeEncodeError) as e:
                print "Unable to encode/decode this line"
                        
if __name__ == "__main__":
    fb = FacebookMessageParser()
    fb.parser()
