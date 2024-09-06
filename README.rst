FlexPrep
========

Container which pre-processes IFS data for `Flexpart <https://www.flexpart.eu/>`_ in the European Weather Cloud (`EWC <https://europeanweather.cloud/>`_).

FlexPrep is part of an automated system triggered by product dissemination via `ECPDS <https://confluence.ecmwf.int/pages/viewpage.action?pageId=228871373>`_ using `Aviso <https://confluence.ecmwf.int/display/EWCLOUDKB/Aviso+Notification+System+on+EWC>`_. This setup ensures that data processing is initiated automatically when new data is available. For detailed instructions on configuring and setting up Aviso for this system, please refer to the `aviso folder in this repository <https://github.com/MeteoSwiss-APN/flexprep/blob/main/aviso/README.md>`_.

Setup Instructions for VM on EWC
================================

To set up the VM for this project, you need to manually add the following files and folders:

1. **VM on EWC with** ``-data`` **layout:** When provisioning the VM on EWC following these `instructions <https://confluence.ecmwf.int/display/EWCLOUDKB/Provision+a+new+instance+-+web>`_, ensure you select the instance with the ``-data`` layout (e.g. ``ubuntu-22.04-data``). This ensures that the Aviso package is automatically installed.

2. `.aviso/` **Folder:** This folder contains configuration and script files required for the AVISO system as described in the `aviso/README.md` file in this repository.

3. `.aws/` **Folder:** This folder contains AWS credentials and configuration files used to authenticate with AWS services.
    
      `credentials` **file:** Contains AWS access keys and secret keys.

           .. code-block:: ini

              [default]
              aws_access_key_id = YOUR_ACCESS_KEY
              aws_secret_access_key = YOUR_SECRET_KEY

      `config` **file (optional):** Contains AWS region and output format settings.

           .. code-block:: ini

              [default]
              region = eu-central-2
              output = json

4. `.env` **File:** This file contains environment variables including S3 bucket access keys and secret keys required by the application.
  
       .. code-block:: bash

          S3_ACCESS_KEY=your-access-key
          S3_SECRET_KEY=your-secret-key