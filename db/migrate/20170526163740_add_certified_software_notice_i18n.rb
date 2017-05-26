class AddCertifiedSoftwareNoticeI18n < ActiveRecord::Migration
  def up
    execute <<-'SQL'
      INSERT INTO public.i18n (key, pt, en) VALUES (
        'certified_software_notice',
        'Emitido por TOConline - https://www.toconline.pt',
        'Created by TOConline - https://www.toconline.pt'
      );
    SQL
  end

  def down
    execute %Q[ DELETE FROM public.i18n WHERE key = 'certified_software_notice' ]
  end
end
