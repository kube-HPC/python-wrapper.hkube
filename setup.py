import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='hkube_python_wrapper',  
     version='0.1',
     scripts=['hkube_python_wrapper'] ,
     author="Hkube",
     author_email="hkube.dev@gmail.com",
     description="Hkube Python Wrapper",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/kube-HPC/python-wrapper.hkube",
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )