from datamule import Portfolio

port = Portfolio('msft')
port.download_submissions(ticker=['MSFT'])