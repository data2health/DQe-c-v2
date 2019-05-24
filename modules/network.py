import datetime
import pandas
import os
import subprocess

class Network:
    def __init__(self, query, org_list=[]):

        self.org_list = org_list
        self.report_folder = "./reports/"


    def createAggregateReports(self):
        self.aggregate_tablelist()
        self.aggregate_missingness()
        self.aggregate_orphan()


    def aggregate_tablelist(self):
        network_report_folder = self.getNetworkReportFolder()

        network_report = self.aggregateLatestSiteReports("tablelist")
        network_report = network_report[["TabNam", "Rows", "TotalSizeKB", "loaded", "organization"]].drop_duplicates()

        network_report.to_csv(network_report_folder + "tablelist_aggregation.csv", index=False)


    def aggregate_missingness(self):
        network_report_folder = self.getNetworkReportFolder()

        network_report = self.aggregateLatestSiteReports("missingness")
        network_report_all = self.aggregateAllSiteReports("missingness")

        network_report.to_csv(network_report_folder + "missingness_aggregation.csv", index=False)
        network_report_all.to_csv(network_report_folder + "all_missingness_aggregation.csv", index=False)


    def aggregate_orphan(self):
        network_report_folder = self.getNetworkReportFolder()

        network_report = self.aggregateLatestSiteReports("orphan")

        network_report.to_csv(network_report_folder + "orphan_aggregation.csv", index=False)


    def aggregateLatestSiteReports(self, reportName):

        DQ_reports = []

        if len(self.org_list) == 0:
            orgs = os.listdir(self.report_folder)
        else:
            orgs = self.org_list
        
            
        for org in orgs:
            dates = []
            for name in os.listdir(self.report_folder + org):
                try:
                    report_date = datetime.datetime.strptime(name, "%m-%d-%Y").date()
                    dates.append(report_date)
                except ValueError:
                    pass
            if len(dates) == 0:
                pass
            else:
                reportFolder = self.report_folder + org + "/" + datetime.datetime.strftime(max(dates), "%m-%d-%Y") + "/"
                try:
                    report = pandas.read_csv(reportFolder + f"{reportName}.csv")
                    if "organization" not in report.columns:
                        report["organization"] = org
                    
                    DQ_reports.append(report)
                except FileNotFoundError:
                    pass

        network_report = pandas.concat(DQ_reports, ignore_index=True)
        return network_report
    
    def aggregateAllSiteReports(self, reportName):

        DQ_reports = []

        if len(self.org_list) == 0:
            orgs = os.listdir(self.report_folder)
        else:
            orgs = self.org_list
        
            
        for org in orgs:
            dates = []
            for name in os.listdir(self.report_folder + org):
                try:
                    report_date = datetime.datetime.strptime(name, "%m-%d-%Y").date()
                    dates.append(report_date)
                except ValueError:
                    pass
            if len(dates) == 0:
                pass
            else:
                for d in dates:
                    reportFolder = self.report_folder + org + "/" + datetime.datetime.strftime(d, "%m-%d-%Y") + "/"
                    try:
                        report = pandas.read_csv(reportFolder + f"{reportName}.csv")
                        
                        DQ_reports.append(report)
                    except FileNotFoundError:
                        pass

        network_report = pandas.concat(DQ_reports, ignore_index=True)
        return network_report


    def getNetworkReportFolder(self):
        ## create the network report folder for today
        network_report_folder = f"./network_reports/{datetime.datetime.today().strftime('%m-%d-%Y')}/"

        if not (os.path.exists(network_report_folder)):
            os.makedirs(network_report_folder)
        
        return network_report_folder

    
    def generateNetworkHTMLReport(self):
        command = 'Rscript'
        scriptPath = './modules/run_network.R'

        outDir = self.getNetworkReportFolder()
        print (outDir)

        subprocess.check_output([command, scriptPath, "-o", outDir], universal_newlines=True)