import os,sys,time

class mylogging():
    def __init__(self,filecwd,filename):
        filedir = os.path.join(filecwd,"log")
        if not os.path.isdir(filedir):
            os.mkdir(filedir)
        print(filedir)
        self.filename = os.path.join(filedir,filename)
        self.file = open(self.filename, "w", buffering=1)

        self.stdout = sys.stdout
        sys.stdout = self.file

    def close(self):
        sys.stdout = self.stdout
        self.file.close()
    
if __name__ == "__main__":
    test = mylogging(os.path.dirname(os.path.abspath(__file__)),"log test.txt")
    print("Hello, 1")
    time.sleep(4)
    print("Hello, 2")
    test.close()