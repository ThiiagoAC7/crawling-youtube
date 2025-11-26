from dotenv import load_dotenv
import os


# list of youtuber names as they appear in their channels url handle
# '@exterminator2000',    # 10
# '@usmilitarytv87',      # 14
# '@usamilitarychannel2', # 17
YTBRS_LIST = [
    '@caseoh_'
    # '@towardseternity', # 21
    # '@usmilitarypowerr',    # 
    # '@nickymgtv',           #
    # '@daftarpopuler',       #
    # '@safanewss',           #
    # '@invoiceindonesia',    #
    # '@smartmultimediavideo',#
    # '@phtvcreatives',       #


]

# top 10 anomalous 15 63 21 43 32 40 52 1
CHANNEL_IDS_LIST = [
    'UCajKgpxKwbuhR2PkI7e-WUA', # @usmilitarypowerr 1
    'UCPubBVDCzu7IWWnitlkEsNw', # @towardseternity  21
    'UCkL2fuuCdjs2g80o-u1DlZA', # @fortressdefense
    'UCZ00nGuJ3BvjPszXFKMwp6g', # @exterminator2000
    'UCRoER7Rmb1P6qXx_o-Zbs8A', # @usmilitarytv87
    'UCpU184Ub1cuTsrHqb1RodjA', # @usamilitarychannel2 
    'UCWDfiSVkdnfcrjA3pLuCoSQ', # @NickyMGTV 15
    'UCQW2tTNJ40V2T6MqV1eMZ0w', # @DaftarPopuler 32
    'UCmvtaFkiWOSHhnOwt4Fz68g', # @safanewss 40
    'UCDg1bjGQOONSRmRSOyTYMdA', # @invoiceindonesia 43
    'UCJTBOZUV_2LaMPQxaeVtgDQ', # @smartmultimediavideo 52
    'UCfdbArIR4ztT0Hjed0jnurA', # @phtvcreatives 63
]


# crawler paths
CRAWLER_PATH = "./data/"
YOUTUBERS_PATH = CRAWLER_PATH+"youtubers.json"


# api key
load_dotenv()
# DEVELOPER_KEY = os.getenv("API_KEY")
# DEVELOPER_KEY = os.getenv("SECONDARY_API_KEY")
DEVELOPER_KEY = os.getenv("THIRD_API_KEY")
