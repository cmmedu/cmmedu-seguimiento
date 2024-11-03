import setuptools

setuptools.setup(
    name="cmmedu_seguimiento",
    version="1.0.1",
    author="Vicente Daie Pinilla",
    author_email="vdaiep@gmail.com",
    description=".",
    url="https://cmmedu.uchile.cl",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "lms.djangoapp": ["cmmedu_seguimiento = cmmedu_seguimiento.apps:CMMEduSeguimiento"],
        "cms.djangoapp": ["cmmedu_seguimiento = cmmedu_seguimiento.apps:CMMEduSeguimiento"]
    },
)